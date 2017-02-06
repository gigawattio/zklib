package testutil

import (
	"os/exec"
	"strings"
	"testing"
	"time"

	zkutil "github.com/gigawattio/zklib/util"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	zkTimeout       = 5 * time.Second
	zkDeleteRetries = 10
)

// withZk allows for a default ZooKeeper address to be specified, and if the
// port is reachable that connection info will be used.  Otherwise a full test
// cluster is launched (NB: this can take 6+ seconds).
func WithZk(t *testing.T, size int, defaultServer string, fn func(zkServers []string)) {
	if size == 1 && len(defaultServer) > 0 {
		checkCmd := exec.Command("nc", append([]string{"-w", "1"}, strings.Split(defaultServer, ":")...)...)
		if err := checkCmd.Run(); err == nil {
			log.Infof("Using already-running default ZooKeeper@%v", defaultServer)
			fn([]string{defaultServer})
			return
		}
	}
	log.Infof("Starting a new ZooKeeper test cluster..")
	WithTestZkCluster(t, size, func(zkServers []string) {
		time.Sleep(100 * time.Millisecond) // Give ZooKeeper a moment to start up.
		fn(zkServers)
	})
}

func ResetZk(t *testing.T, zkServers []string, path string) {
	conn, zkEvents, err := zk.Connect(zkServers, zkTimeout)
	if err != nil {
		t.Fatal(err)
	}
	WhenZkHasSession(zkEvents, func() {
		if err := zkutil.RecursivelyDelete(conn, path, zkDeleteRetries); err != nil {
			t.Fatal(err)
		}
	})
}
