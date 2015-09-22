package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/jaytaylor/uuid"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	Leader   = "leader"
	Follower = "follower"
)

type (
	Node struct {
		Uuid     uuid.UUID
		Hostname string
	}

	Coordinator struct {
		zkServers          []string
		sessionTimeout     time.Duration
		zkCli              *zk.Conn
		eventCh            <-chan zk.Event
		leaderElectionPath string
		localNode          Node
		localNodeJson      []byte
		leaderNode         *Node
		leaderLock         sync.Mutex
		stateLock          sync.Mutex
		stopChan           chan chan struct{}
		subscriberChans    []chan Update    // part of subscription handler.
		subAddChan         chan chan Update // part of subscription handler.
		subRemoveChan      chan chan Update // part of subscription handler.
	}

	Update struct {
		Leader Node
		Mode   string
	}
)

func (node Node) String() string {
	s := fmt.Sprintf("Node{Uuid: %s, Hostname: %s}", node.Uuid.String(), node.Hostname)
	return s
}

// NewCoordinator creates a new cluster client.
//
// leaderElectionPath is the ZooKeeper path to conduct elections under.
func NewCoordinator(zkServers []string, sessionTimeout time.Duration, leaderElectionPath string, subscribers ...chan Update) (*Coordinator, error) {
	// Gather local node info.
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("NewCoordinator: %s", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("NewCoordinator: %s", err)
	}
	localNode := Node{
		Uuid:     *uid,
		Hostname: hostname,
	}
	localNodeJson, err := json.Marshal(&localNode)
	if err != nil {
		return nil, fmt.Errorf("Coordinator.Start: failed converting localNode to JSON: %s", err)
	}

	if subscribers == nil {
		subscribers = []chan Update{}
	}

	cc := &Coordinator{
		zkServers:          zkServers,
		sessionTimeout:     sessionTimeout,
		leaderElectionPath: leaderElectionPath,
		localNode:          localNode,
		localNodeJson:      localNodeJson,
		stopChan:           make(chan chan struct{}),
		subscriberChans:    subscribers,            // part of subscription handler.
		subAddChan:         make(chan chan Update), // part of subscription handler.
		subRemoveChan:      make(chan chan Update), // part of subscription handler.
	}

	return cc, nil
}

func (cc *Coordinator) Start() error {
	log.Info("Coordinator %s starting..", cc.Id())
	cc.stateLock.Lock()
	defer cc.stateLock.Unlock()

	if cc.zkCli != nil {
		return fmt.Errorf("%s: already started", cc.Id())
	}

	// Assemble the cluster coordinator.
	zkCli, eventCh, err := zk.Connect(cc.zkServers, cc.sessionTimeout)
	if err != nil {
		return err
	}
	cc.zkCli = zkCli
	cc.eventCh = eventCh

	// Start the election loop.
	cc.electionLoop()

	log.Info("Coordinator %s started", cc.Id())
	return nil
}

func (cc *Coordinator) Stop() error {
	log.Info("Coordinator %s stopping..", cc.Id())
	cc.stateLock.Lock()
	defer cc.stateLock.Unlock()

	if cc.zkCli == nil {
		return fmt.Errorf("%s: already stopped", cc.Id())
	}

	// Stop the election loop
	ackChan := make(chan struct{})
	cc.stopChan <- ackChan
	<-ackChan // Wait for acknowledgement.

	cc.zkCli.Close()
	cc.zkCli = nil

	log.Info("Coordinator %s stopped", cc.Id())
	return nil
}

func (cc *Coordinator) Id() string {
	id := strings.Split(cc.localNode.Uuid.String(), "-")[0]
	return id
}

// Leader returns the Node representation of the current leader, or nil if there isn't one right now.
// string if the current leader is unknown.
func (cc *Coordinator) Leader() *Node {
	cc.leaderLock.Lock()
	defer cc.leaderLock.Unlock()

	if cc.leaderNode == nil {
		return nil
	}
	// Make a copy of the node to protect against unexpected mutation.
	cp := *cc.leaderNode
	return &cp
}

// Mode returns one of:
//
// "follower" - indicates that this node is not currently the leader.
//
// "leader" - indicates that this node IS the current leader.
func (cc *Coordinator) Mode() string {
	cc.leaderLock.Lock()
	defer cc.leaderLock.Unlock()

	mode := cc.mode()
	return mode
}
func (cc *Coordinator) mode() string {
	if cc.leaderNode == nil {
		return Follower
	}
	itsMe := fmt.Sprintf("%+v", cc.localNode) == fmt.Sprintf("%+v", *cc.leaderNode)
	if itsMe {
		return Leader
	}
	return Follower
}

// createP functions similarly to `mkdir -p`.
func (cc *Coordinator) createP(path string, data []byte, flags int32, acl []zk.ACL) (zxIds []string, err error) {
	zxIds = []string{}
	pieces := strings.Split(strings.Trim(path, "/"), "/")
	var zxId string
	var soFar string
	for _, piece := range pieces {
		soFar += "/" + piece
		if zxId, err = cc.zkCli.Create(soFar, data, flags, acl); err != nil && err != zk.ErrNodeExists {
			return
		}
		zxIds = append(zxIds, zxId)
	}
	err = nil // Clear out any potential error state, since if we made it this far we're OK.
	return
}

// mustCreateP will keep trying to create the path indefinitely.
func (cc *Coordinator) mustCreateP(path string, data []byte, flags int32, acl []zk.ACL) (zxIds []string) {
	var err error
	operation := func() error {
		if zxIds, err = cc.createP(path, []byte{}, 0, acl); err != nil {
			return err
		}
		return nil
	}
	retryUntilSuccess(fmt.Sprintf("%s mustCreateP", cc.Id()), operation, backoff.NewConstantBackOff(50*time.Millisecond))
	return
}

func (cc *Coordinator) mustCreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (zxId string) {
	var err error
	operation := func() error {
		if zxId, err = cc.zkCli.CreateProtectedEphemeralSequential(path, data, acl); err != nil {
			return err
		}
		return nil
	}
	retryUntilSuccess(fmt.Sprintf("%s mustCreateProtectedEphemeralSequential", cc.Id()), operation, backoff.NewConstantBackOff(50*time.Millisecond))
	return
}

func (cc *Coordinator) electionLoop() {
	createElectionZNode := func() (zxId string) {
		log.Debug(cc.Id()+": creating election path=%s", cc.leaderElectionPath)
		zxIds := cc.mustCreateP(cc.leaderElectionPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		log.Debug(cc.Id()+": created election path, zxIds=%+v", zxIds)

		log.Debug(cc.Id() + ": creating protected ephemeral")
		zxId = cc.mustCreateProtectedEphemeralSequential(cc.leaderElectionPath+"/n_", cc.localNodeJson, zk.WorldACL(zk.PermAll))
		log.Debug(cc.Id()+": created protected ephemeral, zxId=%s", zxId)
		return
	}

	mustSubscribe := func(path string) (children []string, stat *zk.Stat, evCh <-chan zk.Event) {
		var err error
		operation := func() error {
			if children, stat, evCh, err = cc.zkCli.ChildrenW(path); err != nil {
				// Protect against infinite failure loop by ensuring the path to watch exists.
				createElectionZNode()
				return err
			}
			return nil
		}
		log.Debug(cc.Id()+": setting watch on path=%s", cc.leaderElectionPath)
		retryUntilSuccess(fmt.Sprintf("%s mustSubscribe", cc.Id()), operation, backoff.NewConstantBackOff(50*time.Millisecond))
		log.Debug(cc.Id()+": successfully set watch on path=%s", cc.leaderElectionPath)
		return
	}

	go func() {
		// var children []string
		var childCh <-chan zk.Event
		var zxId string // Most recent zxid.

		setWatch := func() {
			_ /*children*/, _, childCh = mustSubscribe(cc.leaderElectionPath)
		}

		notifySubscribers := func(updateInfo Update) {
			if nSub := len(cc.subscriberChans); nSub > 0 {
				log.Debug(cc.Id()+": broadcasting leader update to %v subscribers", nSub)
				for _, subChan := range cc.subscriberChans {
					if subChan != nil {
						go func(subChan chan Update) {
							subChan <- updateInfo
						}(subChan)
					}
				}
			}
		}

		checkLeader := func() {
			var children []string
			var stat *zk.Stat
			operation := func() error {
				var err error
				if children, stat, err = cc.zkCli.Children(cc.leaderElectionPath); err != nil {
					return err
				}
				return nil
			}
			retryUntilSuccess("checkLeader", operation, backoff.NewConstantBackOff(50*time.Millisecond))
			log.Notice(cc.Id()+": checkLeader: children=%+v, stat=%+v", children, *stat)
			min := -1
			var minChild string
			for _, child := range children {
				pieces := strings.Split(child, "-n_")
				if len(pieces) <= 1 {
					continue
				}
				n, err := strconv.Atoi(pieces[1])
				if err != nil {
					log.Debug(cc.Id()+": Failed to parse child=%s: %s, skipping child", child, err)
					continue
				}
				if min == -1 || n < min {
					min = n
					minChild = child
				}
			}
			if min == -1 {
				log.Warning(cc.Id()+": No valid children found in children=%+v, aborting check", children)
				return
			}
			minChild = cc.leaderElectionPath + "/" + minChild
			data, stat, err := cc.zkCli.Get(minChild)
			if err != nil {
				log.Error(cc.Id()+": Error checking leader znode path=%s: %s", minChild, err)
			}
			log.Notice(cc.Id()+": Discovered leader znode at %s, data=%s stat=%+v", minChild, string(data), *stat)

			var leaderNode Node
			if err := json.Unmarshal(data, &leaderNode); err != nil {
				log.Error(cc.Id()+": Failed parsing Node from JSON=%s: %s", string(data), err)
			}

			cc.leaderLock.Lock()
			cc.leaderNode = &leaderNode
			cc.leaderLock.Unlock()

			updateInfo := Update{
				Leader: leaderNode,
				Mode:   cc.mode(),
			}
			notifySubscribers(updateInfo)
		}

		for {
			// Add a new watch as per the behavior outlined at
			// http://zookeeper.apache.org/doc/r3.4.1/zookeeperProgrammers.html#ch_zkWatches.

			// log.Debug(cc.Id() + ": watch children=%+v", children)
			select {
			case ev := <-cc.eventCh: // Watch connection events.
				if ev.Err != nil {
					log.Error(cc.Id()+": eventCh: error: %s", ev.Err)
					continue
				}
				log.Debug(cc.Id()+": eventCh: received event=%+v", ev)
				if ev.Type == zk.EventSession && ev.State == zk.StateConnected {
					zxId = createElectionZNode()
					log.Notice(cc.Id()+": new zxId=%s", zxId)
					setWatch()
					checkLeader()
				}

			case ev := <-childCh: // Watch election path.
				if ev.Err != nil {
					log.Error(cc.Id()+": childCh: watcher error %+v", ev.Err)
				}
				if ev.Type == zk.EventNodeChildrenChanged {
					checkLeader()
				}
				setWatch()
				log.Debug(cc.Id()+": childCh: ev.Path=%s ev=%+v", ev.Path, ev)

				// case <-time.After(time.Second * 5):
				// 	log.Info(cc.Id() + ": childCh: Child watcher timed out")

			case subChan := <-cc.subAddChan: // Add subscriber chan.
				log.Debug(cc.Id() + ": received subscriber add request")
				cc.subscriberChans = append(cc.subscriberChans, subChan)

			case unsubChan := <-cc.subRemoveChan: // Remove subscriber chan.
				log.Debug(cc.Id() + ": received subscriber removal request")
				revisedChans := []chan Update{}
				for _, ch := range cc.subscriberChans {
					if ch != unsubChan {
						revisedChans = append(revisedChans, ch)
					}
				}
				cc.subscriberChans = revisedChans

			case ackChan := <-cc.stopChan: // Stop loop.
				log.Debug(cc.Id() + ": election loop received stop request")
				ackChan <- struct{}{} // Acknowledge stop.
				log.Debug(cc.Id() + ": election loop exiting")
				return
			}
		}
	}()
}

// Subscribe adds a channel to the slice of subscribers who get notified when
// the leader changes.
func (cc *Coordinator) Subscribe(subChan chan Update) {
	cc.subAddChan <- subChan
}

// Unsubscribe removes a channel frmo the slice of subscribers.
func (cc *Coordinator) Unsubscribe(unsubChan chan Update) {
	cc.subRemoveChan <- unsubChan
}
