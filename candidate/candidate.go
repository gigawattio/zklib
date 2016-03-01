package candidate

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"gigawatt-common/pkg/concurrency"
	zkutil "gigawatt-common/pkg/zk/util"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	LeaderChanSize = 6
)

var (
	AlreadyRegisteredError  = errors.New("already registered")
	NotRegisteredError      = errors.New("not registered")
	NotEnrolledError        = errors.New("candidate is not presently enrolled in the election")
	InvalidChildrenLenError = errors.New("0 children listed afer created protected ephemeral sequential - this should never happen")
)

var (
	worldAllAcl = zk.WorldACL(zk.PermAll)
)

type Candidate struct {
	ElectionPath     string
	Node             *Node
	zxId             string
	zxIdLock         sync.RWMutex
	registered       bool
	registrationLock sync.Mutex
	stopChan         chan chan struct{}
	Debug            bool
}

func NewCandidate(electionPath string, node *Node) *Candidate {
	candidate := &Candidate{
		ElectionPath: electionPath,
		Node:         node,
		stopChan:     make(chan chan struct{}, 1),
	}
	return candidate
}

func (c *Candidate) Registered() bool {
	c.registrationLock.Lock()
	defer c.registrationLock.Unlock()
	return c.registered
}

func (c *Candidate) Register(conn *zk.Conn) (<-chan *Node, error) {
	log.Info("[uuid=%v] Candidate registering..", c.Node.Uuid)

	c.registrationLock.Lock()
	defer c.registrationLock.Unlock()

	if c.registered {
		return nil, AlreadyRegisteredError
	}
	c.registered = true

	enroll := func() (leader *Node, watch <-chan zk.Event, err error) {
		log.Info("[uuid=%v] Candidate enrolling..", c.Node.Uuid)

		var (
			zxId     string
			data     []byte
			children []string
		)

		if zxId, err = c.validZxId(conn); err != nil {
			return
		}
		if zxId == "" {
			if err = c.ensureElectionPathExists(conn); err != nil {
				return
			}
			if data, err = json.Marshal(c.Node); err != nil {
				err = fmt.Errorf("serializing node data: %s", err)
				return
			}
			// log.Debug("CPES for node=%v", string(data))
			if zxId, err = conn.CreateProtectedEphemeralSequential(c.ElectionPath+"/n_", data, worldAllAcl); err != nil {
				err = fmt.Errorf("creating protected ephemeral sequential %q: %s", c.ElectionPath, err)
				return
			}
			if idx := strings.LastIndex(zxId, "/"); idx > 0 {
				zxId = zxId[idx+1:]
			} else {
				err = fmt.Errorf("received invalid zxid=%v", zxId)
				return
			}
			if c.Debug {
				log.Debug("[uuid=%v] Candidate has new zxId=%v", c.Node.Uuid, zxId)
			}
			c.zxIdLock.Lock()
			c.zxId = zxId
			c.zxIdLock.Unlock()
		}

		if children, _, watch, err = conn.ChildrenW(c.ElectionPath); err != nil {
			return
		}
		sort.Sort(ZxIds(children))
		if c.Debug {
			log.Debug("[uuid=%v] Candidate children=%v", c.Node.Uuid, children)
		}

		if children[0] == zxId {
			leader = c.Node
		} else if leader, err = c.getNode(conn, children[0]); err != nil {
			return
		}

		log.Info("[uuid=%v] Candidate enrolled OK", c.Node.Uuid)
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
						log.Error("[uuid=%v] Candidate watcher enrollment err=%s", c.Node.Uuid, result.err)
						time.Sleep(1 * time.Second) // TODO: Definitely use backoff here.
						continue
					} else if result.leader != leader {
						leader = result.leader

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
						log.Debug("[uuid=%v] Candidate watcher detected watch chan closed, triggering enroll", c.Node.Uuid)
					}
					watch = nil
					continue
				}

				switch event.Type {
				case zk.EventNodeDeleted, zk.EventNotWatching, zk.EventNodeChildrenChanged:
					if c.Debug {
						log.Debug("[uuid=%v] Candidate watcher received event=%+v, triggering enroll", c.Node.Uuid, event)
					}
					watch = nil

				default:
					if c.Debug {
						log.Debug("[uuid=%v] Candidate watcher received misc event=%+v", c.Node.Uuid, event)
					}
				}

			case ackCh := <-c.stopChan:
				close(leaderChan)
				c.wipeZxId(conn)
				ackCh <- struct{}{}
				return
			}
		}
	}()

	log.Info("[uuid=%v] Candidate registered OK", c.Node.Uuid)
	return leaderChan, nil
}

func (c *Candidate) wipeZxId(conn *zk.Conn) {
	c.zxIdLock.Lock()
	defer c.zxIdLock.Unlock()

	path := c.ElectionPath + "/" + c.zxId
	exists, stat, err := conn.Exists(path)
	if err != nil {
		log.Warning("[uuid=%v] Candidate zxId=%v deletion skipped due to existence check err=%s", c.Node.Uuid, c.zxId, err)
	} else if exists {
		if err = conn.Delete(path, stat.Version); err != nil {
			log.Warning("[uuid=%v] Candidate zxId=%v deletion failed due to delete err=%s", c.Node.Uuid, c.zxId, err)
		} else {
			log.Info("[uuid=%v] Candidate successfully deleted zxId path=%v", c.Node.Uuid, path)
		}
	} else {
		log.Info("[uuid=%v] Candidate zxId=%v deletion skipped due to !exists", c.Node.Uuid, c.zxId)
	}
}

func (c *Candidate) Unregister() error {
	log.Info("[uuid=%v] Candidate unregistering..", c.Node.Uuid)

	c.registrationLock.Lock()
	defer c.registrationLock.Unlock()

	if !c.registered {
		return NotRegisteredError
	}
	c.registered = false

	ackCh := make(chan struct{})
	c.stopChan <- ackCh
	<-ackCh

	log.Info("[uuid=%v] Candidate unregistered", c.Node.Uuid)
	return nil
}

func (c *Candidate) validZxId(conn *zk.Conn) (zxId string, err error) {
	c.zxIdLock.RLock()
	zxId = c.zxId
	c.zxIdLock.RUnlock()

	if zxId == "" {
		return
	}

	var (
		path   = c.ElectionPath + "/" + zxId
		exists bool
	)

	if exists, _, err = conn.Exists(path); err != nil {
		err = fmt.Errorf("checking if zxId=%v exists: %s", path, err)
	} else if !exists {
		zxId = ""
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
			return fmt.Errorf("creating electionPath=%v", c.ElectionPath, err)
		}
	}
	return nil
}

func (c *Candidate) getNode(conn *zk.Conn, zxId string) (node *Node, err error) {
	var data []byte
	if data, _, err = conn.Get(c.ElectionPath + "/" + zxId); err != nil {
		err = fmt.Errorf("getting node data for zxId=%v: %s", zxId, err)
		return
	}
	node = &Node{}
	if err = json.Unmarshal(data, node); err != nil {
		err = fmt.Errorf("deserializing node data for zxId=%v: %s", zxId, err)
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

	for _, zxId := range children {
		func(zxId string) {
			collectorFunc := func() (err error) {
				var node *Node
				if node, err = c.getNode(conn, zxId); err != nil {
					return
				}
				participantsLock.Lock()
				participants = append(participants, *node)
				participantsLock.Unlock()
				return
			}
			collectorFuncs = append(collectorFuncs, collectorFunc)
		}(zxId)
	}

	if err = concurrency.MultiGo(collectorFuncs...); err != nil {
		return
	}
	return
}

// func (c *Candidate) ZxId() string {
// 	c.zxIdLock.RLock()
// 	defer c.zxIdLock.RUnlock()
// 	return c.zxId
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
