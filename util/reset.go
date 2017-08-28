package util

import (
	"time"

	zktestutil "github.com/gigawattio/zklib/testutil"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	zkTimeout       = 5 * time.Second
	zkDeleteRetries = 10
)

func ResetZk(zkServers []string, path string) error {
	conn, zkEvents, err := zk.Connect(zkServers, zkTimeout)
	if err != nil {
		return err
	}
	zktestutil.WhenZkHasSession(zkEvents, func() {
		err = RecursivelyDelete(conn, path, zkDeleteRetries)
	})
	if err != nil {
		return err
	}
	return nil
}
