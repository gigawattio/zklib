package dmutex

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gigawattio/zklib/cluster"
	"github.com/gigawattio/zklib/cluster/primitives"
	zkutil "github.com/gigawattio/zklib/util"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/samuel/go-zookeeper/zk"
)

type DistributedMutexService struct {
	zkServers     []string // ZooKeeper host/port pairs.
	clientTimeout time.Duration
	basePath      string
	coordinators  map[string]*cluster.Coordinator
	localLock     sync.Mutex
}

var (
	DistributedMutexAcquisitionFailed = errors.New("DistributedMutexService: operation already in progress")
	DistributedMutexNoLeader          = errors.New("DistributedMutexService: unable to obtain lock-info")
)

func NewDistributedMutexService(zkServers []string, clientTimeout time.Duration, basePath string) *DistributedMutexService {
	service := &DistributedMutexService{
		zkServers:     zkServers,
		clientTimeout: clientTimeout,
		basePath:      zkutil.NormalizePath(basePath),
		coordinators:  map[string]*cluster.Coordinator{},
	}
	return service
}

func (service *DistributedMutexService) Lock(objectId string, data string, expireTimeout time.Duration) error {
	if strings.Contains(objectId, "/") {
		return fmt.Errorf("DistributedMutexService: invalid objectId=%v, must not contain '/'", objectId)
	}

	service.localLock.Lock()
	coordinator, ok := service.coordinators[objectId]
	service.localLock.Unlock()

	if ok {
		log.Infof("Operation already in progress for objectId=%v (my mode=%v, leader=%+v)", objectId, coordinator.Mode(), coordinator.Leader())
		return fmt.Errorf("%s: %v", DistributedMutexAcquisitionFailed, coordinator.LeaderData())
	} else {
		path := fmt.Sprintf("%v/%v", service.basePath, objectId)
		coordinator, err := cluster.NewCoordinator(service.zkServers, service.clientTimeout, path, data)
		if err != nil {
			return fmt.Errorf("DistributedMutexService lock: %s", err)
		}
		if err := coordinator.Start(); err != nil {
			return fmt.Errorf("DistributedMutexService start: %s", err)
		}

		// Wait for there to be a leader.
		var leader *primitives.Node
		waitingForLeaderSince := time.Now()
		operation := func() error {
			if leader = coordinator.Leader(); leader == nil {
				return DistributedMutexNoLeader
			}
			return nil
		}
		if err := backoff.Retry(operation, service.backoffStrategy()); err != nil {
			log.Warnf("Failed to obtain leader info for objectId=%v after %s", objectId, time.Now().Sub(waitingForLeaderSince))
			if err := coordinator.Stop(); err != nil {
				log.Warnf("Problem stopping coordinator for objectId=%v (non-fatal, will continue): %s", objectId, err)
			}
			return DistributedMutexNoLeader
		} else {
			log.Infof("Obtained leader info for objectId=%v after %s", objectId, time.Now().Sub(waitingForLeaderSince))
		}

		if coordinator.Mode() != primitives.Leader {
			if err := coordinator.Stop(); err != nil {
				log.Warnf("Problem stopping coordinator for objectId=%v (non-fatal, will continue): %s", objectId, err)
			}
			log.Infof("Operation already in progress for objectId=%v (my mode=follower, leader=%+v)", objectId, coordinator.Leader())
			return fmt.Errorf("%s: %v", DistributedMutexAcquisitionFailed, coordinator.LeaderData())
		}
		service.localLock.Lock()
		if existing, ok := service.coordinators[objectId]; ok {
			service.localLock.Unlock()
			return fmt.Errorf("%s: %v", DistributedMutexAcquisitionFailed, existing.LeaderData())
		}
		service.coordinators[objectId] = coordinator
		service.localLock.Unlock()

		log.Infof("[id=%v] Successfully acquired lock for objectId=%v", coordinator.Id(), objectId)
		if expireTimeout.Nanoseconds() != int64(0) {
			go service.autoExpire(coordinator.Id(), objectId, expireTimeout)
		}
	}

	return nil
}

func (service *DistributedMutexService) Unlock(objectId string) error {
	service.localLock.Lock()
	coordinator, ok := service.coordinators[objectId]
	service.localLock.Unlock()

	if ok {
		id := coordinator.Id()
		if err := coordinator.Stop(); err != nil {
			log.Warnf("[id=%v] Stopping coordinator failed for objectId=%v (non-fatal, will continue): %s", id, objectId, err)
		}
		service.localLock.Lock()
		delete(service.coordinators, objectId)
		service.localLock.Unlock()
		log.Infof("[id=%v] Lock removed for objectId=%v", id, objectId)
	} else {
		log.Debugf("[id=%v] No lock found for objectId=%v", coordinator.Id(), objectId)
	}

	return nil
}

func (service *DistributedMutexService) List() ([]string, error) {
	mutexes := []string{}
	err := zkutil.WithZkSession(service.zkServers, service.clientTimeout, func(conn *zk.Conn) error {
		path := zkutil.NormalizePath(service.basePath)

		var err error
		if mutexes, _, err = conn.Children(path); err != nil {
			if err == zk.ErrNoNode {
				return nil
			}
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return mutexes, nil
}

func (service *DistributedMutexService) Clean() error {
	err := zkutil.WithZkSession(service.zkServers, service.clientTimeout, func(conn *zk.Conn) error {
		path := zkutil.NormalizePath(service.basePath)

		znodes, _, err := conn.Children(path)
		if err == zk.ErrNoNode {
			return nil
		}
		if err != nil {
			return err
		}
		for _, znode := range znodes {
			lockPath := fmt.Sprintf("%v/%v", zkutil.NormalizePath(path), znode)
			children, stat, err := conn.Children(lockPath)
			if err != nil && err == zk.ErrNoNode {
				continue
			} else if err != nil {
				return err
			} else if len(children) == 0 {
				log.Debugf("Removing %v", lockPath)
				if err := conn.Delete(lockPath, stat.Version); err != nil {
					if err == zk.ErrNotEmpty {
						// Check for ZK claiming it's not empty when it actually is.
						check, _, checkErr := conn.Children(lockPath)
						if checkErr == nil {
							if len(check) == 0 {
								log.Errorf("ZK is refusing to delete %q claiming it is not empty, but it appears empty; RESTARTING ZOOKEEPER SERVER IS RECOMMENDED IF THIS MESSAGE IS REPEATED ACROSS RUNS", lockPath)
							} else {
								continue // There is some apparent activity on the lock.
							}
						}
					}
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// autoExpire should be invoked asynchronously.
func (service *DistributedMutexService) autoExpire(id string, objectId string, expireTimeout time.Duration) {
	time.Sleep(expireTimeout)

	service.localLock.Lock()
	coordinator, ok := service.coordinators[objectId]
	service.localLock.Unlock()

	if !ok {
		log.Debugf("[id=%v] No lock found for objectId=%v", id, objectId)
		return
	}
	if coordinator.Id() != id {
		log.Infof("[id=%v] Lock for objectId=%v has a different id=%v (will not auto-expire)", id, objectId, coordinator.Id())
		return
	}
	log.Infof("[id=%v] Auto-expiring lock for objectId=%v since it's been more than the requested expiry of %s", id, objectId, expireTimeout)
	if err := coordinator.Stop(); err != nil {
		log.Warnf("[id=%v] Stopping coordinator failed for objectId=%v (non-fatal, will continue): %s", id, objectId, err)
	}
	service.localLock.Lock()
	delete(service.coordinators, objectId)
	service.localLock.Unlock()
	log.Infof("[id=%v] Lock removed for objectId=%v", id, objectId)
}

func (service *DistributedMutexService) backoffStrategy() *backoff.ExponentialBackOff {
	strategy := backoff.NewExponentialBackOff()
	strategy.InitialInterval = 1 * time.Millisecond
	strategy.Multiplier = 1.5
	strategy.MaxInterval = 250 * time.Millisecond
	strategy.MaxElapsedTime = 5 * time.Second
	return strategy
}

func IsAcquisitionFailedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), DistributedMutexAcquisitionFailed.Error())
}
