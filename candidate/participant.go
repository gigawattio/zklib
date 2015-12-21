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
	// UpgradeFunc   func() error
	// DowngradeFunc func() error
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

	participant = Participant{
		Config:     config,
		EventsChan: eventsChan,
		StopChan:   stopChan,
	}

	zkConnect := func() error {
		log.Info("Connecting to ZooKeeper servers=%v", config.ZkAddrs)

	Retry:
		select {
		case <-stopChan:
			log.Info("Stop request received")
			return errorlib.NotRunningError
		default:
		}

		conn, events, err := zk.Connect(config.ZkAddrs, config.ZkTimeout)
		if err != nil {
			log.Error("ZooKeeper connection failed (will retry): %s", err)
			time.Sleep(1 * time.Second) // TODO: Use backoff here.
			goto Retry
		}
		log.Info("Opened connection to ZooKeeper")
		zkCli = conn
		zkEvents = events
		return nil
	}

	if err := zkConnect(); err != nil {
		log.Info("EventLoop exiting from initial connect due to stop request")
		return
	}

	go func() {
		for {
			select {
			case leader, ok := <-leaderChan:
				if !ok {
					log.Notice("Leader chan closed, switching to fake leader")
					leaderChan = fakeLeaderChan
					// TODO(jet): Possibly add deregistration here?
					continue
				}

				log.Notice("New leader received: %+v", leader)
				if leader == candidate.Node {
					eventsChan <- LeaderUpgradeEvent
				} else {
					eventsChan <- DowngradeEvent
				}

			case event, ok := <-zkEvents: // ZooKeeper connection events.
				if !ok {
					log.Debug("Detected zkEvents chan close")
					continue
				}
				if event.Err != nil {
					log.Error("zkEvents: error: %s", event.Err)
					continue
				}
				log.Debug("zkEvents: received event=%+v", event)
				if event.Type == zk.EventSession {
					switch event.State {
					case zk.StateConnected, zk.StateHasSession:
						if event.State == zk.StateHasSession {
							eventsChan <- ConnectedEvent
							log.Notice("Connected state detected, state=%v type=%v", event.State.String(), event.Type)

							newLeaderChan, err := candidate.Register(zkCli)
							if err != nil {
								panic(fmt.Sprintf("unexpected candidate registration failure: %s", err))
							}
							leaderChan = newLeaderChan
							log.Info("Candidate registration initiated")
						}

					case zk.StateDisconnected, zk.StateExpired:
						log.Notice("Unconnected state detected, state=%v type=%v", event.State.String(), event.Type)
						eventsChan <- DisconnectedEvent
						if candidate.Registered() {
							if err := candidate.Unregister(); err != nil {
								panic(fmt.Sprintf("unexpected candidate unregistration failure: %s", err))
							}
						}
					}
				}

			case <-stopChan:
				if err := candidate.Unregister(); err != nil {
					log.Error("Unexpected candidate unregistration failure: %s", err)
				}
				log.Info("Participant exiting due to stop request")
				return
			}
		}
	}()

	return
}
