package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gigawattio/concurrency"
	"github.com/gigawattio/gentle"
	"github.com/gigawattio/zklib/cluster/primitives"
	"github.com/gigawattio/zklib/util"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"
)

var (
	backoffDuration = 50 * time.Millisecond
)

type Coordinator struct {
	zkServers              []string
	sessionTimeout         time.Duration
	zkCli                  *zk.Conn
	eventCh                <-chan zk.Event
	leaderElectionPath     string
	LocalNode              primitives.Node
	localNodeJson          []byte
	leaderNode             *primitives.Node
	leaderLock             sync.Mutex
	membershipRequestsChan chan chan clusterMembershipResponse
	stateLock              sync.Mutex
	stopChan               chan chan struct{}
	subscriberChans        []chan primitives.Update    // part of subscription handler.
	subAddChan             chan chan primitives.Update // part of subscription handler.
	subRemoveChan          chan chan primitives.Update // part of subscription handler.
}

type clusterMembershipResponse struct {
	nodes []primitives.Node
	err   error
}

// NewCoordinator creates a new cluster client.
//
// leaderElectionPath is the ZooKeeper path to conduct elections under.
func NewCoordinator(zkServers []string, sessionTimeout time.Duration, leaderElectionPath string, data string, subscribers ...chan primitives.Update) (*Coordinator, error) {
	// Gather local node info.
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("NewCoordinator: %s", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("NewCoordinator: %s", err)
	}

	localNode := primitives.Node{
		Uuid:     uid,
		Hostname: hostname,
		Data:     data,
	}
	localNodeJson, err := json.Marshal(&localNode)
	if err != nil {
		return nil, fmt.Errorf("NewCoordinator: failed converting localNode to JSON: %s", err)
	}

	if subscribers == nil {
		subscribers = []chan primitives.Update{}
	}

	cc := &Coordinator{
		zkServers:              zkServers,
		sessionTimeout:         sessionTimeout,
		leaderElectionPath:     leaderElectionPath,
		LocalNode:              localNode,
		localNodeJson:          localNodeJson,
		membershipRequestsChan: make(chan chan clusterMembershipResponse),
		stopChan:               make(chan chan struct{}),
		subscriberChans:        subscribers,                       // part of subscription handler.
		subAddChan:             make(chan chan primitives.Update), // part of subscription handler.
		subRemoveChan:          make(chan chan primitives.Update), // part of subscription handler.
	}

	return cc, nil
}

func (cc *Coordinator) Start() error {
	log.Infof("Coordinator Id=%v starting..", cc.Id())
	cc.stateLock.Lock()
	defer cc.stateLock.Unlock()

	if cc.zkCli != nil {
		return fmt.Errorf("%v: already started", cc.Id())
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

	log.Infof("Coordinator Id=%v started", cc.Id())
	return nil
}

func (cc *Coordinator) Stop() error {
	log.Infof("Coordinator Id=%v stopping..", cc.Id())
	cc.stateLock.Lock()
	defer cc.stateLock.Unlock()

	if cc.zkCli == nil {
		return fmt.Errorf("%v: already stopped", cc.Id())
	}

	// Stop the election loop
	ackChan := make(chan struct{})
	cc.stopChan <- ackChan
	<-ackChan // Wait for acknowledgement.

	cc.zkCli.Close()
	cc.zkCli = nil

	log.Infof("Coordinator Id=%v stopped", cc.Id())
	return nil
}

func (cc *Coordinator) Id() (id string) {
	defer func() {
		if r := recover(); r != nil {
			log.Warnf("Recovered from panic: %s", r)
		}
	}()
	id = strings.Split(cc.LocalNode.Uuid.String(), "-")[0]
	return
}

// Leader returns the Node representation of the current leader, or nil if there isn't one right now.
// string if the current leader is unknown.
func (cc *Coordinator) Leader() *primitives.Node {
	cc.leaderLock.Lock()
	defer cc.leaderLock.Unlock()

	if cc.leaderNode == nil {
		return nil
	}
	// Make a copy of the node to protect against unexpected mutation.
	cp := *cc.leaderNode
	return &cp
}

func (cc *Coordinator) LeaderData() string {
	cc.leaderLock.Lock()
	defer cc.leaderLock.Unlock()

	if cc.leaderNode == nil {
		return ""
	}
	return cc.leaderNode.Data
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
		return primitives.Follower
	}
	itsMe := fmt.Sprintf("%+v", cc.LocalNode) == fmt.Sprintf("%+v", *cc.leaderNode)
	if itsMe {
		return primitives.Leader
	}
	return primitives.Follower
}

func (cc *Coordinator) Members() (nodes []primitives.Node, err error) {
	request := make(chan clusterMembershipResponse)
	cc.membershipRequestsChan <- request
	select {
	case response := <-request:
		if err = response.err; err != nil {
			return
		}
		nodes = response.nodes
	case <-time.After(cc.sessionTimeout):
		err = fmt.Errorf("membership request timed out after %v", cc.sessionTimeout)
	}
	return
}

func (cc *Coordinator) electionLoop() {
	createElectionZNode := func() (zNode string) {
		log.Debugf("%v: creating election path=%v", cc.Id(), cc.leaderElectionPath)
		strategy := backoff.NewConstantBackOff(backoffDuration)
		zNodes := util.MustCreateP(cc.zkCli, cc.leaderElectionPath, []byte{}, 0, zk.WorldACL(zk.PermAll), strategy)
		log.Debugf("%v: created election path, zNodes=%+v", cc.Id(), zNodes)

		log.Debugf("%v: creating protected ephemeral", cc.Id())
		strategy = backoff.NewConstantBackOff(backoffDuration)
		zNode = util.MustCreateProtectedEphemeralSequential(cc.zkCli, cc.leaderElectionPath+"/n_", cc.localNodeJson, zk.WorldACL(zk.PermAll), strategy)
		log.Debugf("%v: created protected ephemeral, zNode=%v", cc.Id(), zNode)
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
		log.Debugf("%v: setting watch on path=%v", cc.Id(), cc.leaderElectionPath)
		gentle.RetryUntilSuccess(fmt.Sprintf("%v mustSubscribe", cc.Id()), operation, backoff.NewConstantBackOff(backoffDuration))
		log.Debugf("%v: successfully set watch on path=%v", cc.Id(), cc.leaderElectionPath)
		return
	}

	go func() {
		// var children []string
		var (
			childCh <-chan zk.Event
			zNode   string // Most recent zxid.
		)

		setWatch := func() {
			_ /*children*/, _, childCh = mustSubscribe(cc.leaderElectionPath)
		}

		notifySubscribers := func(updateInfo primitives.Update) {
			if nSub := len(cc.subscriberChans); nSub > 0 {
				log.Debugf("%v: broadcasting leader update to %v subscribers", cc.Id(), nSub)
				for _, subChan := range cc.subscriberChans {
					if subChan != nil {
						select {
						case subChan <- updateInfo:
						default:
						}
					}
				}
			}
		}

		checkLeader := func() {
			var (
				children  []string
				stat      *zk.Stat
				operation = func() error {
					var err error
					if children, stat, err = cc.zkCli.Children(cc.leaderElectionPath); err != nil {
						return err
					}
					return nil
				}
				min      = -1
				minChild string
			)
			gentle.RetryUntilSuccess("checkLeader", operation, backoff.NewConstantBackOff(50*time.Millisecond))
			log.Debugf("%v: checkLeader: children=%+v, stat=%+v", cc.Id(), children, *stat)
			for _, child := range children {
				pieces := strings.Split(child, "-n_")
				if len(pieces) <= 1 {
					continue
				}
				n, err := strconv.Atoi(pieces[1])
				if err != nil {
					log.Debugf("%v: Failed to parse child=%v: %s, skipping child", cc.Id(), child, err)
					continue
				}
				if min == -1 || n < min {
					min = n
					minChild = child
				}
			}
			if min == -1 {
				log.Warnf("%v: No valid children found in children=%+v, aborting check", cc.Id(), children)
				return
			}
			minChild = cc.leaderElectionPath + "/" + minChild
			data, stat, err := cc.zkCli.Get(minChild)
			if err != nil {
				log.Error("%v: Error checking leader znode path=%v: %s", cc.Id(), minChild, err)
			}
			log.Debugf("%v: Discovered leader znode at %v, data=%v stat=%+v", cc.Id(), minChild, string(data), *stat)

			var leaderNode primitives.Node
			if err := json.Unmarshal(data, &leaderNode); err != nil {
				log.Error("%v: Failed parsing Node from JSON=%v: %s", cc.Id(), string(data), err)
			}

			cc.leaderLock.Lock()
			cc.leaderNode = &leaderNode
			cc.leaderLock.Unlock()

			updateInfo := primitives.Update{
				Leader: leaderNode,
				Mode:   cc.mode(),
			}
			notifySubscribers(updateInfo)
		}

		for {
			// Add a new watch as per the behavior outlined at
			// http://zookeeper.apache.org/doc/r3.4.1/zookeeperProgrammers.html#ch_zkWatches.

			// log.Debugf("%v: watch children=%+v",cc.Id(), children)
			select {
			case ev := <-cc.eventCh: // Watch connection events.
				if ev.Err != nil {
					log.Error("%v: eventCh: error: %s", cc.Id(), ev.Err)
					continue
				}
				log.Debugf("%v: eventCh: received event=%+v", cc.Id(), ev)
				if ev.Type == zk.EventSession {
					switch ev.State {
					case zk.StateHasSession:
						zNode = createElectionZNode()
						log.Debugf("%v: new zNode=%v", cc.Id(), zNode)
						setWatch()
						checkLeader()
					}
				}

			case ev := <-childCh: // Watch election path.
				if ev.Err != nil {
					log.Error("%v: childCh: watcher error %+v", cc.Id(), ev.Err)
				}
				if ev.Type == zk.EventNodeChildrenChanged {
					checkLeader()
				}
				setWatch()
				log.Debugf("%v: childCh: ev.Path=%v ev=%+v", cc.Id(), ev.Path, ev)

				// case <-time.After(time.Second * 5):
				// 	log.Infof("%v: childCh: Child watcher timed out",cc.Id())

			case requestChan := <-cc.membershipRequestsChan:
				cc.handleMembershipRequest(requestChan)

			case subChan := <-cc.subAddChan: // Add subscriber chan.
				log.Debugf("%v: received subscriber add request", cc.Id())
				cc.subscriberChans = append(cc.subscriberChans, subChan)

			case unsubChan := <-cc.subRemoveChan: // Remove subscriber chan.
				log.Debugf("%v: received subscriber removal request", cc.Id())
				revisedChans := []chan primitives.Update{}
				for _, ch := range cc.subscriberChans {
					if ch != unsubChan {
						revisedChans = append(revisedChans, ch)
					}
				}
				cc.subscriberChans = revisedChans

			case ack := <-cc.stopChan: // Stop loop.
				log.Debugf("%v: election loop received stop request", cc.Id())
				ack <- struct{}{} // Acknowledge stop.
				log.Debugf("%v: election loop exiting", cc.Id())
				return
			}
		}
	}()
}

func (cc *Coordinator) handleMembershipRequest(requestChan chan clusterMembershipResponse) {
	children, _, err := cc.zkCli.Children(cc.leaderElectionPath)
	if err != nil {
		requestChan <- clusterMembershipResponse{err: err}
		return
	}
	var (
		numChildren = len(children)
		nodeGetters = make([]func() error, numChildren)
		nodes       = make([]primitives.Node, numChildren)
		nodesLock   sync.Mutex
	)
	for i, child := range children {
		func(i int, child string) {
			nodeGetters[i] = func() error {
				data, _, err := cc.zkCli.Get(cc.leaderElectionPath + "/" + child)
				if err != nil {
					return err
				}
				var node primitives.Node
				if err := json.Unmarshal(data, &node); err != nil {
					return fmt.Errorf("decoding %v bytes of JSON for child=%v: %s", len(data), child, err)
				}
				nodesLock.Lock()
				nodes[i] = node
				nodesLock.Unlock()
				return nil
			}
		}(i, child)
	}
	if err := concurrency.MultiGo(nodeGetters...); err != nil {
		requestChan <- clusterMembershipResponse{err: err}
		return
	}
	requestChan <- clusterMembershipResponse{nodes: nodes}
}

// Subscribe adds a channel to the slice of subscribers who get notified when
// the leader changes.
func (cc *Coordinator) Subscribe(subChan chan primitives.Update) {
	cc.subAddChan <- subChan
}

// Unsubscribe removes a channel frmo the slice of subscribers.
func (cc *Coordinator) Unsubscribe(unsubChan chan primitives.Update) {
	cc.subRemoveChan <- unsubChan
}
