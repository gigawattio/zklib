package candidate

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gigawattio/concurrency"
	zkutil "github.com/gigawattio/zklib/util"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

const (
	LeaderChanSize = 6
)

var (
	AlreadyRegisteredError  = errors.New("already registered")
	NotRegisteredError      = errors.New("not registered")
	NotEnrolledError        = errors.New("candidate is not presently enrolled in the election")
	InvalidChildrenLenError = errors.New("0 children listed afer created protected ephemeral sequential - this should never happen")

	DefaultConnCheckFrequency = 5 * time.Second // Default frequency of connection checks.
)

var (
	worldAllAcl = zk.WorldACL(zk.PermAll)
)

type Candidate struct {
	ElectionPath       string
	Node               *Node
	ConnCheckFrequency time.Duration // Frequency of connection checks.
	zNode              string
	zNodeLock          sync.RWMutex
	registered         bool
	registrationLock   sync.Mutex
	stopChan           chan chan struct{}
	Debug              bool
}

func New(electionPath string, node *Node) *Candidate {
	candidate := &Candidate{
		ElectionPath:       electionPath,
		Node:               node,
		ConnCheckFrequency: DefaultConnCheckFrequency,
		stopChan:           make(chan chan struct{}, 1),
	}
	return candidate
}

func (c *Candidate) Registered() bool {
	c.registrationLock.Lock()
	defer c.registrationLock.Unlock()
	return c.registered
}

func (c *Candidate) Register(conn *zk.Conn) (<-chan *Node, error) {
	log.Infof("[uuid=%v] Candidate registering..", c.Node.Uuid)

	c.registrationLock.Lock()
	defer c.registrationLock.Unlock()

	if c.registered {
		return nil, AlreadyRegisteredError
	}
	c.registered = true

	enroll := func() (leader *Node, watch <-chan zk.Event, err error) {
		log.Infof("[uuid=%v] Candidate enrolling..", c.Node.Uuid)

		var (
			zNode    string
			data     []byte
			children []string
		)

		if zNode, err = c.validZNode(conn); err != nil {
			return
		}
		if zNode == "" {
			if err = c.ensureElectionPathExists(conn); err != nil {
				return
			}
			if data, err = json.Marshal(c.Node); err != nil {
				err = fmt.Errorf("serializing node data: %s", err)
				return
			}
			// log.Debugf("CPES for node=%v", string(data))
			if zNode, err = conn.CreateProtectedEphemeralSequential(c.ElectionPath+"/n_", data, worldAllAcl); err != nil {
				err = fmt.Errorf("creating protected ephemeral sequential %q: %s", c.ElectionPath, err)
				return
			}
			if idx := strings.LastIndex(zNode, "/"); idx > 0 {
				zNode = zNode[idx+1:]
			} else {
				err = fmt.Errorf("received invalid zxid=%v", zNode)
				return
			}
			if c.Debug {
				log.Debugf("[uuid=%v] Candidate has new zNode=%v", c.Node.Uuid, zNode)
			}
			c.zNodeLock.Lock()
			c.zNode = zNode
			c.zNodeLock.Unlock()
		}

		if children, _, watch, err = conn.ChildrenW(c.ElectionPath); err != nil {
			return
		}
		sort.Sort(ZNodes(children))
		if c.Debug {
			log.Debugf("[uuid=%v] Candidate children=%v", c.Node.Uuid, children)
		}

		if children[0] == zNode {
			leader = c.Node
		} else if leader, err = c.getNode(conn, children[0]); err != nil {
			return
		}

		log.Infof("[uuid=%v] Candidate enrolled OK", c.Node.Uuid)
		return
	}

	type asyncEnrollResult struct {
		leader *Node
		watch  <-chan zk.Event
		err    error
	}

	asyncEnroll := func() chan *asyncEnrollResult {
		resultChan := make(chan *asyncEnrollResult)
		go func() {
			leader, watch, err := enroll()
			resultChan <- &asyncEnrollResult{
				leader: leader,
				watch:  watch,
				err:    err,
			}
		}()
		return resultChan
	}

	leaderChan := make(chan *Node, LeaderChanSize)

	// Watcher.
	go func() {
		var (
			leader *Node
			watch  <-chan zk.Event
		)

		for {
			if watch == nil {
				select {
				case result := <-asyncEnroll():
					if result.err != nil {
						log.Errorf("[uuid=%v] Candidate watcher enrollment err=%s", c.Node.Uuid, result.err)
						time.Sleep(1 * time.Second) // TODO: Definitely use backoff here.
						continue
					} else if result.leader != leader {
						leader = result.leader

						if c.Debug && leader != nil {
							log.Debugf("[uuid=%v] Sending updated leader=%+v", c.Node.Uuid, *leader)
						}

						select {
						case leaderChan <- leader:
							// pass
						default:
						}
					}
					watch = result.watch

				case ackCh := <-c.stopChan:
					close(leaderChan)
					ackCh <- struct{}{}
					return
				}
			}

			select {
			case event, ok := <-watch:
				if !ok {
					if c.Debug {
						log.Debugf("[uuid=%v] Candidate watcher detected watch chan closed", c.Node.Uuid)
					}
					watch = nil
					continue
				}

				switch event.Type {
				case zk.EventNodeDeleted, zk.EventNotWatching, zk.EventNodeChildrenChanged:
					if c.Debug {
						log.Debugf("[uuid=%v] Candidate watcher received event=%+v, triggering enroll", c.Node.Uuid, event)
					}
					watch = nil

				default:
					if c.Debug {
						log.Debugf("[uuid=%v] Candidate watcher received misc event=%+v", c.Node.Uuid, event)
					}
				}

			case <-time.After(c.ConnCheckFrequency):
				if leader != nil {
					state := conn.State()
					if state == zk.StateConnecting || state == zk.StateDisconnected || state == zk.StateExpired || state == zk.StateAuthFailed || state == zk.StateUnknown {
						// When connection state is deemed unhealthy, reset the values for watch
						// and leader.
						watch = nil
						leader = nil

						if c.Debug {
							log.Debugf("[uuid=%v] Unhealthy connection state deteted, values for leader and watch have been reset", c.Node.Uuid)
						}

						select {
						case leaderChan <- leader:
							// pass
						default:
						}
					}
				}

			case ackCh := <-c.stopChan:
				close(leaderChan)
				c.wipeZNode(conn)
				ackCh <- struct{}{}
				return
			}
		}
	}()

	log.Infof("[uuid=%v] Candidate registered OK", c.Node.Uuid)
	return leaderChan, nil
}

func (c *Candidate) wipeZNode(conn *zk.Conn) {
	c.zNodeLock.Lock()
	defer c.zNodeLock.Unlock()

	path := c.ElectionPath + "/" + c.zNode
	exists, stat, err := conn.Exists(path)
	if err != nil {
		log.Warnf("[uuid=%v] Candidate zNode=%v deletion skipped due to existence check err=%s", c.Node.Uuid, c.zNode, err)
	} else if exists {
		if err = conn.Delete(path, stat.Version); err != nil {
			log.Warnf("[uuid=%v] Candidate zNode=%v deletion failed due to delete err=%s", c.Node.Uuid, c.zNode, err)
		} else {
			log.Infof("[uuid=%v] Candidate successfully deleted zNode path=%v", c.Node.Uuid, path)
		}
	} else {
		log.Infof("[uuid=%v] Candidate zNode=%v deletion skipped due to !exists", c.Node.Uuid, c.zNode)
	}
}

func (c *Candidate) Unregister() error {
	log.Infof("[uuid=%v] Candidate unregistering..", c.Node.Uuid)

	c.registrationLock.Lock()
	defer c.registrationLock.Unlock()

	if !c.registered {
		return NotRegisteredError
	}
	c.registered = false

	ackCh := make(chan struct{})
	c.stopChan <- ackCh
	<-ackCh

	log.Infof("[uuid=%v] Candidate unregistered", c.Node.Uuid)
	return nil
}

func (c *Candidate) validZNode(conn *zk.Conn) (zNode string, err error) {
	c.zNodeLock.RLock()
	zNode = c.zNode
	c.zNodeLock.RUnlock()

	if zNode == "" {
		return
	}

	var (
		path   = c.ElectionPath + "/" + zNode
		exists bool
	)

	if exists, _, err = conn.Exists(path); err != nil {
		err = fmt.Errorf("checking if zNode=%v exists: %s", path, err)
	} else if !exists {
		zNode = ""
	}
	return
}

func (c *Candidate) ensureElectionPathExists(conn *zk.Conn) error {
	exists, _, err := conn.Exists(c.ElectionPath)
	if err != nil {
		return fmt.Errorf("checking if electionPath=%v exists: %s", c.ElectionPath, err)
	}
	if !exists {
		if _, err = zkutil.CreateP(conn, c.ElectionPath, []byte{}, 0, worldAllAcl); err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("creating electionPath=%v: %s", c.ElectionPath, err)
		}
	}
	return nil
}

func (c *Candidate) getNode(conn *zk.Conn, zNode string) (node *Node, err error) {
	var data []byte
	if data, _, err = conn.Get(c.ElectionPath + "/" + zNode); err != nil {
		err = fmt.Errorf("getting node data for zNode=%v: %s", zNode, err)
		return
	}
	node = &Node{}
	if err = json.Unmarshal(data, node); err != nil {
		err = fmt.Errorf("deserializing node data for zNode=%v: %s", zNode, err)
		return
	}
	return
}

func (c *Candidate) Participants(conn *zk.Conn) (participants []Node, err error) {
	var (
		children []string
	)
	if children, err = c.children(conn); err != nil {
		return
	}

	var (
		numChildren      = len(children)
		collectorFuncs   = make([]func() error, 0, numChildren)
		participantsLock sync.Mutex
	)
	participants = make([]Node, 0, numChildren)

	for _, zNode := range children {
		func(zNode string) {
			collectorFunc := func() (err error) {
				var node *Node
				if node, err = c.getNode(conn, zNode); err != nil {
					return
				}
				participantsLock.Lock()
				participants = append(participants, *node)
				participantsLock.Unlock()
				return
			}
			collectorFuncs = append(collectorFuncs, collectorFunc)
		}(zNode)
	}

	if err = concurrency.MultiGo(collectorFuncs...); err != nil {
		return
	}
	return
}

// func (c *Candidate) ZNode() string {
// 	c.zNodeLock.RLock()
// 	defer c.zNodeLock.RUnlock()
// 	return c.zNode
// }

func (c *Candidate) children(conn *zk.Conn) (children []string, err error) {
	if children, _, err = conn.Children(c.ElectionPath); err != nil {
		err = fmt.Errorf("listing children: %s", err)
		return
	}
	if len(children) == 0 {
		err = InvalidChildrenLenError
		return
	}
	sort.Strings(children)
	return
}
