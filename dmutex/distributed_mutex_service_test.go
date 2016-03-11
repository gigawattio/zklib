package dmutex_test

import (
	"fmt"
	"testing"
	"time"

	"gigawatt-common/pkg/testlib"
	"gigawatt-common/pkg/zk/dmutex"
	zktestutil "gigawatt-common/pkg/zk/testutil"
	zkutil "gigawatt-common/pkg/zk/util"

	"github.com/samuel/go-zookeeper/zk"
)

func Test_DistributedMutexService(t *testing.T) {
	zkPath := fmt.Sprintf("/%v", testlib.CurrentRunningTest())
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		defer zktestutil.ResetZk(t, zkServers, zkPath)
		service := dmutex.NewDistributedMutexService(zkServers, 5*time.Second, zkPath)

		objectId1 := "my-app-1"
		timeout := 3 * time.Second
		if err := service.Lock(objectId1, timeout); err != nil {
			t.Fatal(err)
		}
		if err := service.Lock(objectId1, timeout); err != dmutex.DistributedMutexAcquisitionFailed {
			t.Fatalf("Locked object failure error did not match expected `dmutex.DistributedMutexAcquisitionFailed': %s", err)
		}

		objectId2 := "my-app-2"
		if err := service.Lock(objectId2, timeout); err != nil {
			t.Fatal(err)
		}

		if err := service.Unlock(objectId1); err != nil {
			t.Fatal(err)
		}
		if err := service.Lock(objectId1, timeout); err != nil {
			t.Fatal(err)
		}
		if err := service.Unlock(objectId1); err != nil {
			t.Fatal(err)
		}
	})
}

func Test_DistributedMutexServiceCleaner(t *testing.T) {
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		var (
			timeout  = 3 * time.Second
			zkPath   = fmt.Sprintf("/%v/", testlib.CurrentRunningTest())
			objectId = func(id interface{}) string {
				return fmt.Sprintf("my-app-%v", id)
			}
			service = dmutex.NewDistributedMutexService(zkServers, 5*time.Second, zkPath)
		)

		defer zktestutil.ResetZk(t, zkServers, zkPath)

		cleanAndVerifyNumChildren := func(expected int) error {
			if err := service.Clean(); err != nil {
				return fmt.Errorf("Unexpected error from Clean(): %s", err)
			}
			err := zkutil.WithZkSession(zkServers, timeout, func(conn *zk.Conn) error {
				children, _, err := conn.Children(zkutil.NormalizePath(zkPath))
				t.Logf("children=%v err=%s\n", children, err)
				if err != nil && err != zk.ErrNoNode {
					return err
				}
				if actual := len(children); actual != expected {
					return fmt.Errorf("Expected num children under %q == %v but actual count is %v", zkPath, expected, actual)
				}
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		}

		if err := cleanAndVerifyNumChildren(0); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(1), timeout); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(1); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(1), timeout); err != dmutex.DistributedMutexAcquisitionFailed {
			t.Fatalf("Locked object failure error did not match expected `dmutex.DistributedMutexAcquisitionFailed': %s", err)
		}
		if err := cleanAndVerifyNumChildren(1); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(2), timeout); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(2); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(3), timeout); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(3); err != nil {
			t.Fatal(err)
		}

		if err := service.Unlock(objectId(1)); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(2); err != nil {
			t.Fatal(err)
		}

		if err := service.Unlock(objectId(3)); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(1); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(1), timeout); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(2); err != nil {
			t.Fatal(err)
		}

		if err := service.Unlock(objectId(2)); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(1); err != nil {
			t.Fatal(err)
		}

		if err := service.Unlock(objectId(1)); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(0); err != nil {
			t.Fatal(err)
		}
	})
}
