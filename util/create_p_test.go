package util

import (
	"testing"
	"time"

	"github.com/gigawattio/zklib/testutil"

	"github.com/cenkalti/backoff"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	backoffDuration = 5 * time.Millisecond
)

func TestCreateP(t *testing.T) {
	testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
		conn, zkEvents, err := zk.Connect(zkServers, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		testutil.WhenZkHasSession(zkEvents, func() {
			path := "/TestCreateP"
			if err := RecursivelyDelete(conn, path); err != nil {
				t.Fatal(err)
			}
			if _, err := CreateP(conn, path, []byte("hi"), 0, zk.WorldACL(zk.PermAll)); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func TestMustCreateP(t *testing.T) {
	testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
		conn, zkEvents, err := zk.Connect(zkServers, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		done := make(chan struct{}, 1)

		go testutil.WhenZkHasSession(zkEvents, func() {
			defer func() { done <- struct{}{} }()
			path := "/TestMustCreateP"
			if err := RecursivelyDelete(conn, path); err != nil {
				t.Fatal(err)
			}
			MustCreateP(conn, path, []byte("hi"), 0, zk.WorldACL(zk.PermAll), backoff.NewConstantBackOff(backoffDuration))
			return
		})

		timeout := 5 * time.Second

		select {
		case <-done:
			// pass
		case <-time.After(timeout):
			t.Fatalf("Timed out after %s", timeout)
		}
	})
}

func TestMustCreateProtectedEphemeralSequential(t *testing.T) {
	testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
		conn, zkEvents, err := zk.Connect(zkServers, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		done := make(chan struct{}, 1)

		go testutil.WhenZkHasSession(zkEvents, func() {
			defer func() { done <- struct{}{} }()
			path := "/TestMustCreateProtectedEphemeralSequential"
			if err := RecursivelyDelete(conn, path); err != nil {
				t.Fatal(err)
			}
			strategy := backoff.NewConstantBackOff(backoffDuration)
			MustCreateP(conn, path, []byte("hi"), 0, zk.WorldACL(zk.PermAll), strategy)
			strategy = backoff.NewConstantBackOff(backoffDuration)
			zNode := MustCreateProtectedEphemeralSequential(conn, path, []byte("hi"), zk.WorldACL(zk.PermAll), strategy)
			t.Logf("zNode=%v", zNode)
			return
		})

		timeout := 5 * time.Second

		select {
		case <-done:
			// pass
		case <-time.After(timeout):
			t.Fatalf("Timed out after %s", timeout)
		}
	})
}
