package candidate

// Simple ZooKeeper leadership API.

import (
	"fmt"
	"time"

	"gigawatt-common/pkg/errorlib"

	"github.com/samuel/go-zookeeper/zk"
)

type ParticipantConfig struct {
	ElectionPath string
	ZkAddrs      []string
	ZkTimeout    time.Duration
	Node         *Node
	Debug        bool
}

type Event int

const EventsChanSize = 10

const (
	ConnectedEvent = iota
	DisconnectedEvent
	LeaderUpgradeEvent
	DowngradeEvent
)

type Participant struct {
	Config     ParticipantConfig
	EventsChan <-chan Event
	StopChan   chan<- struct{}
}

func Participate(config ParticipantConfig) (participant Participant) {
	var (
		candidate      = NewCandidate(config.ElectionPath, config.Node)
		zkEvents       <-chan zk.Event
		zkCli          *zk.Conn
		fakeLeaderChan = make(<-chan *Node)
		leaderChan     = fakeLeaderChan
		eventsChan     = make(chan Event, EventsChanSize)
		stopChan       = make(chan struct{})
	)

	candidate.Debug = config.Debug

	participant = Participant{
		Config:     config,
		EventsChan: eventsChan,
		StopChan:   stopChan,
	}

	zkConnect := func() error {
		log.Info("[uuid=%v] Connecting to ZooKeeper servers=%v", candidate.Node.Uuid, config.ZkAddrs)

	Retry:
		select {
		case <-stopChan:
			if config.Debug {
				log.Debug("Stop request received")
			}
			return errorlib.NotRunningError
		default:
		}

		conn, events, err := zk.Connect(config.ZkAddrs, config.ZkTimeout)
		if err != nil {
			log.Error("[uuid=%v] ZooKeeper connection failed (will retry): %s", candidate.Node.Uuid, err)
			time.Sleep(1 * time.Second) // TODO: Use backoff here.
			goto Retry
		}
		log.Info("[uuid=%v] Opened connection to ZooKeeper", candidate.Node.Uuid)
		zkCli = conn
		zkEvents = events
		return nil
	}

	if err := zkConnect(); err != nil {
		if config.Debug {
			log.Debug("[uuid=%v] EventLoop exiting from initial connect due to stop request", candidate.Node.Uuid)
		}
		return
	}

	go func() {
		for {
			select {
			case leader, ok := <-leaderChan:
				if !ok {
					if config.Debug {
						log.Debug("[uuid=%v] Leader chan closed, switching to fake leader", candidate.Node.Uuid)
					}
					leaderChan = fakeLeaderChan
					// TODO(jet): Possibly add deregistration here?
					continue
				}

				log.Notice("[uuid=%v] New leader received: %+v", candidate.Node.Uuid, leader)
				if leader == candidate.Node {
					eventsChan <- LeaderUpgradeEvent
				} else {
					eventsChan <- DowngradeEvent
				}

			case event, ok := <-zkEvents: // ZooKeeper connection events.
				if !ok {
					if config.Debug {
						log.Debug("[uuid=%v] Detected zkEvents chan close", candidate.Node.Uuid)
					}
					continue
				}
				if event.Err != nil {
					log.Error("[uuid=%v] zkEvents: error: %s", candidate.Node.Uuid, event.Err)
					continue
				}
				if config.Debug {
					log.Debug("[uuid=%v] zkEvents: received event=%+v", candidate.Node.Uuid, event)
				}
				if event.Type == zk.EventSession {
					switch event.State {
					case zk.StateConnected, zk.StateHasSession:
						if event.State == zk.StateHasSession {
							eventsChan <- ConnectedEvent
							if config.Debug {
								log.Debug("[uuid=%v] Connected state detected, state=%v type=%v", candidate.Node.Uuid, event.State.String(), event.Type)
							}

							newLeaderChan, err := candidate.Register(zkCli)
							if err != nil {
								panic(fmt.Sprintf("unexpected candidate registration failure: %s", err))
							}
							leaderChan = newLeaderChan
							if config.Debug {
								log.Debug("[uuid=%v] Candidate registration initiated", candidate.Node.Uuid)
							}
						}

					case zk.StateDisconnected, zk.StateExpired:
						log.Info("[uuid=%v] Disconnected from ZooKeeper", candidate.Node.Uuid)
						if config.Debug {
							log.Debug("[uuid=%v] Unconnected state detected, state=%v type=%v", candidate.Node.Uuid, event.State.String(), event.Type)
						}
						eventsChan <- DisconnectedEvent
						if candidate.Registered() {
							if err := candidate.Unregister(); err != nil {
								panic(fmt.Sprintf("unexpected candidate unregistration failure: %s", err))
							}
						}
					}
				}

			case <-stopChan:
				if config.Debug {
					log.Debug("[uuid=%v] Participant exiting due to stop request", candidate.Node.Uuid)
				}
				if err := candidate.Unregister(); err != nil && config.Debug {
					log.Warning("Unexpected candidate unregistration failure: %s", err)
				}
				return
			}
		}
	}()

	return
}
