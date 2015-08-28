package util

import (
	"fmt"
	"testing"

	"github.com/samuel/go-zookeeper/zk"
)

type testLogger struct {
	t      *testing.T
	prefix string
	suffix string
}

func (tl *testLogger) Write(bs []byte) (n int, err error) {
	tl.t.Logf("%v%v%v", tl.prefix, string(bs), tl.suffix)
	n = len(bs)
	return
}

func WithTestZkCluster(t *testing.T, size int, fn func(zkServers []string)) {
	var (
		stdout = &testLogger{t: t, prefix: "\033[90m[stdout] ", suffix: "\033[0m"}
		stderr = &testLogger{t: t, prefix: "\033[90m[stderr] ", suffix: "\033[0m"}
	)
	tc, err := zk.StartTestCluster(size, stdout, stderr)
	if err != nil {
		t.Fatalf("Starting ZooKeeper test cluster: %s", err)
	}
	defer func() {
		if err := tc.Stop(); err != nil {
			t.Errorf("Stopping ZooKeeper test cluster: %s", err)
		}
	}()

	servers := make([]string, len(tc.Servers))
	t.Logf("Started ZooKeeper test cluster with %v node(s)", len(tc.Servers))
	for i, server := range tc.Servers {
		t.Logf("\tServer #%v listening on 127.0.0.1:%v", i+1, server.Port)
		servers[i] = fmt.Sprintf("127.0.0.1:%v", server.Port)
	}

	fn(servers)
}
