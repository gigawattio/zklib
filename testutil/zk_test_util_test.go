package testutil

import (
	"testing"
)

func TestNestedUsage(t *testing.T) {
	WithTestZkCluster(t, 1, func(zkServers []string) {
		t.Log("Inside WithTestZkCluster Function #1")
		WithTestZkCluster(t, 1, func(zkServers []string) {
			t.Log("Inside WithTestZkCluster Function #2")
		})
	})
}
