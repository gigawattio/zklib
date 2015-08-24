package dmutex

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gigawatt-server/pkg/zk/cluster"

	"github.com/cenkalti/backoff"
)

type DistributedMutexService struct {
	zooKeepers    []string // ZooKeeper host/port pairs.
	clientTimeout time.Duration
	basePath      string
	coordinators  map[string]*cluster.Coordinator
	localLock     sync.Mutex
}

var (
	DistributedMutexAcquisitionFailed = errors.New("DistributedMutexService: operation already in progress")
	DistributedMutexNoLeader          = errors.New("DistributedMutexService: unable to obtain lock-info")
)

func NewDistributedMutexService(zooKeepers []string, clientTimeout time.Duration, basePath string) *DistributedMutexService {
	if !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}
	service := &DistributedMutexService{
		zooKeepers:    zooKeepers,
		clientTimeout: clientTimeout,
		basePath:      basePath,
		coordinators:  map[string]*cluster.Coordinator{},
	}
	return service
}

func (service *DistributedMutexService) Lock(objectId string, expireTimeout time.Duration) error {
	if strings.Contains(objectId, "/") {
		return fmt.Errorf("DistributedMutexService: invalid objectId=%v, must not contain '/'", objectId)
	}

	service.localLock.Lock()
	defer service.localLock.Unlock()

	if coordinator, ok := service.coordinators[objectId]; ok {
		log.Notice("Operation already in progress for objectId=%v (my mode=%v, leader=%+v)", objectId, coordinator.Mode(), coordinator.Leader())
		return DistributedMutexAcquisitionFailed
	} else {
		path := service.basePath + objectId
		coordinator, err := cluster.NewCoordinator(service.zooKeepers, service.clientTimeout, path)
		if err != nil {
			return fmt.Errorf("DistributedMutexService lock: %s", err)
		}
		if err := coordinator.Start(); err != nil {
			return fmt.Errorf("DistributedMutexService start: %s", err)
		}

		// Wait for there to be a leader.
		var leader *cluster.Node
		waitingForLeaderSince := time.Now()
		operation := func() error {
			if leader = coordinator.Leader(); leader == nil {
				return DistributedMutexNoLeader
			}
			return nil
		}
		if err := backoff.Retry(operation, service.backoffStrategy()); err != nil {
			log.Warning("Failed to obtain leader info for objectId=%v after %s", objectId, time.Now().Sub(waitingForLeaderSince))
			if err := coordinator.Stop(); err != nil {
				log.Warning("Problem stopping coordinator for objectId=%v (non-fatal, will continue): %s", objectId, err)
			}
			return DistributedMutexNoLeader
		} else {
			log.Info("Obtained leader info for objectId=%v after %s", objectId, time.Now().Sub(waitingForLeaderSince))
		}

		if coordinator.Mode() != cluster.Leader {
			if err := coordinator.Stop(); err != nil {
				log.Warning("Problem stopping coordinator for objectId=%v (non-fatal, will continue): %s", objectId, err)
			}
			log.Notice("Operation already in progress for objectId=%v (my mode=follower, leader=%+v)", objectId, coordinator.Leader())
			return DistributedMutexAcquisitionFailed
		}
		service.coordinators[objectId] = coordinator

		go service.autoExpire(coordinator.Id(), objectId, expireTimeout)
	}

	return nil
}

func (service *DistributedMutexService) Unlock(objectId string) error {
	service.localLock.Lock()
	defer service.localLock.Unlock()

	if coordinator, ok := service.coordinators[objectId]; ok {
		if err := coordinator.Stop(); err != nil {
			log.Warning("[id=%v] Stopping coordinator failed for objectId=%v (non-fatal, will continue): %s", coordinator.Id(), objectId, err)
		}
		delete(service.coordinators, objectId)
		log.Notice("[id=%v] Lock removed for objectId=%v", coordinator.Id(), objectId)
	} else {
		log.Debug("No lock found for objectId=%v", objectId)
	}

	return nil
}

// autoExpire should be invoked asynchronously.
func (service *DistributedMutexService) autoExpire(id string, objectId string, expireTimeout time.Duration) {
	time.Sleep(expireTimeout)

	service.localLock.Lock()
	defer service.localLock.Unlock()

	coordinator, ok := service.coordinators[objectId]
	if !ok {
		log.Debug("[id=%v] No lock found for objectId=%v", id, objectId)
		return
	}
	if coordinator.Id() != id {
		log.Info("[id=%v] Lock for objectId=%v has a different id=%v (will not auto-expire)", id, objectId, coordinator.Id())
		return
	}
	log.Notice("[id=%v] Auto-expiring lock for objectId=%v since it's been more than the requested expiry of %s", id, objectId, expireTimeout)
	if err := coordinator.Stop(); err != nil {
		log.Warning("[id=%v] Stopping coordinator failed for objectId=%v (non-fatal, will continue): %s", id, objectId, err)
	}
	delete(service.coordinators, objectId)
	log.Notice("[id=%v] Lock removed for objectId=%v", id, objectId)
}

func (service *DistributedMutexService) backoffStrategy() *backoff.ExponentialBackOff {
	strategy := backoff.NewExponentialBackOff()
	strategy.InitialInterval = 1 * time.Millisecond
	strategy.Multiplier = 1.5
	strategy.MaxInterval = 250 * time.Millisecond
	strategy.MaxElapsedTime = 5 * time.Second
	return strategy
}
