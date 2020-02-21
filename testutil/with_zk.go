package testutil

import (
	"net"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	zkTimeout = 5 * time.Second
)

// WithZk allows for a default ZooKeeper address to be specified, and if the
// port is reachable that connection info will be used.  Otherwise a full test
// cluster is launched (NB: this can take 6+ seconds).
func WithZk(t *testing.T, size int, defaultServer string, fn func(zkServers []string)) {
	if size == 1 && len(defaultServer) > 0 {
		conn, err := net.DialTimeout("tcp", defaultServer, zkTimeout)
		if err == nil {
			conn.Close()
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
