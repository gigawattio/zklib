package dmutex

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func Test_DistributedMutexService(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	tc, err := zk.StartTestCluster(1, stdout, stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer func(tc *zk.TestCluster) {
		tc.Stop()

		// Show ZK logs only if there was a problem.
		if t.Failed() {
			t.Logf("zk stdout: %s", stdout.String())
			t.Logf("zk stderr: %s", stderr.String())
		}
	}(tc)

	zkServers := make([]string, len(tc.Servers))
	for i := 0; i < len(zkServers); i++ {
		zkServers[i] = fmt.Sprintf("127.0.0.1:%v", tc.Servers[i].Port)
	}
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

	if err := tc.Stop(); err != nil {
		t.Fatal(err)
	}
}
