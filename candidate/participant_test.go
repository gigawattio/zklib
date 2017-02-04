package candidate_test

import (
	"testing"
	"time"

	"github.com/gigawattio/zklib/candidate"
	zktestutil "github.com/gigawattio/zklib/testutil"
	zkutil "github.com/gigawattio/zklib/util"

	"github.com/samuel/go-zookeeper/zk"
)

func TestParticipant(t *testing.T) {
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		conn, zkEvents, err := zk.Connect(zkServers, zkTimeout)
		if err != nil {
			t.Fatal(err)
		}

		zktestutil.WhenZkHasSession(zkEvents, func() {
			if err := zkutil.RecursivelyDelete(conn, electionPath, zkDeleteRetries); err != nil {
				t.Fatal(err)
			}

			participant := candidate.Participate(candidate.ParticipantConfig{
				ElectionPath: electionPath,
				ZkAddrs:      zkServers,
				ZkTimeout:    zkTimeout,
				Node:         candidate.NewNode("12345", "localhost", 0, nil),
			})

			startedAt := time.Now()

			select {
			case event := <-participant.EventsChan:
				if event == candidate.LeaderUpgradeEvent {
					break
				}

			case <-time.After(1 * time.Second):
				if time.Now().After(startedAt.Add(5 * time.Second)) {
					t.Fatalf("Timed out after 5 seconds waiting for leadership confirmation")
				}
			}

			participant.StopChan <- struct{}{}
		})
	})
}
