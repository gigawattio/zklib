package dmutex_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gigawattio/testlib"
	"github.com/gigawattio/zklib/dmutex"
	zktestutil "github.com/gigawattio/zklib/testutil"
	zkutil "github.com/gigawattio/zklib/util"

	"github.com/samuel/go-zookeeper/zk"
)

func Test_DistributedMutexService(t *testing.T) {
	zkPath := fmt.Sprintf("/%v", testlib.CurrentRunningTest())
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		defer func() {
			if err := zkutil.ResetZk(zkServers, zkPath); err != nil {
				t.Error(err)
			}
		}()
		service := dmutex.NewDistributedMutexService(zkServers, 5*time.Second, zkPath)

		objectId1 := "my-app-1"
		timeout := 3 * time.Second
		if err := service.Lock(objectId1, "1", timeout); err != nil {
			t.Fatal(err)
		}
		if err := service.Lock(objectId1, "2", timeout); !dmutex.IsAcquisitionFailedError(err) {
			t.Fatalf("Locked object failure error did not match expected `dmutex.DistributedMutexAcquisitionFailed': %s", err)
		}

		objectId2 := "my-app-2"
		if err := service.Lock(objectId2, "3", timeout); err != nil {
			t.Fatal(err)
		}

		if err := service.Unlock(objectId1); err != nil {
			t.Fatal(err)
		}
		if err := service.Lock(objectId1, "4", timeout); err != nil {
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

		defer func() {
			if err := zkutil.ResetZk(zkServers, strings.TrimRight(zkPath, "/")); err != nil {
				t.Errorf("Unexpected error while resetting zkPath=%q: %s", zkPath, err)
			}
		}()

		cleanAndVerifyNumChildren := func(expected int) error {
			if err := service.Clean(); err != nil {
				return fmt.Errorf("Unexpected error from Clean(): %s", err)
			}
			err := zkutil.WithZkSession(zkServers, timeout, func(conn *zk.Conn) error {
				children, _, err := conn.Children(zkutil.NormalizePath(zkPath))
				if err != nil && err != zk.ErrNoNode {
					t.Logf("children=%v err=%s\n", children, err)
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

		if err := service.Lock(objectId(1), "1", timeout); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(1); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(1), "2", timeout); !dmutex.IsAcquisitionFailedError(err) {
			t.Fatalf("Locked object failure error did not match expected `dmutex.DistributedMutexAcquisitionFailed': %s", err)
		}
		if err := cleanAndVerifyNumChildren(1); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(2), "3", timeout); err != nil {
			t.Fatal(err)
		}
		if err := cleanAndVerifyNumChildren(2); err != nil {
			t.Fatal(err)
		}

		if err := service.Lock(objectId(3), "4", timeout); err != nil {
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

		if err := service.Lock(objectId(1), "5", timeout); err != nil {
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
