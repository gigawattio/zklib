package dmutex

import (
	"testing"
	"time"

	"gigawatt-common/pkg/zk/testutil"
)

func Test_DistributedMutexService(t *testing.T) {
	testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
		service := NewDistributedMutexService(zkServers, 5*time.Second, "/test/")

		objectId1 := "my-app-1"
		timeout := 3 * time.Second
		if err := service.Lock(objectId1, timeout); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if err := service.Lock(objectId1, timeout); err != DistributedMutexAcquisitionFailed {
			t.Fatalf("Locked object failure: %s", err)
		}

		objectId2 := "my-app-2"
		if err := service.Lock(objectId2, timeout); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		if err := service.Unlock(objectId1); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if err := service.Lock(objectId1, timeout); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if err := service.Unlock(objectId1); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	})
}
