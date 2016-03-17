package cluster_test

import (
	"sync"
	"testing"
	"time"

	"gigawatt-common/pkg/zk/cluster"
	"gigawatt-common/pkg/zk/testutil"
)

var zkTimeout = 1 * time.Second

// ncc creates a new Coordinator for a given test cluster.
func ncc(t *testing.T, zkServers []string, subscribers ...chan cluster.Update) *cluster.Coordinator {
	cc, err := cluster.NewCoordinator(zkServers, zkTimeout, "/comorgnet/election", subscribers...)
	if err != nil {
		t.Fatal(err)
	}
	if err := cc.Start(); err != nil {
		t.Fatal(err)
	}
	return cc
}

func TestClusterLeaderElection(t *testing.T) {
	// NB: tcSz == zookeeper test cluster size.
	for _, tcSz := range []int{1} {
		testutil.WithTestZkCluster(t, tcSz, func(zkServers []string) {
			for _, sz := range []int{1, 2, 3, 4} {
				t.Logf("Testing with number of cluster members sz=%v", sz)

				members := make([]*cluster.Coordinator, sz)
				for i := 0; i < sz; i++ {
					cc := ncc(t, zkServers)
					members[i] = cc

					go func(i int) {
						if err := cc.Stop(); err != nil {
							t.Fatalf("Stopping cc member #%v: %s", i, err)
						}

						wait := time.Duration(i*250) * time.Millisecond
						t.Logf("random wait for member=%s --> %s", cc.Id(), wait)
						time.Sleep(wait)

						if err := cc.Start(); err != nil {
							t.Fatalf("Starting cc member #%v: %s", i, err)
						}
					}(i)
				}

				time.Sleep(time.Duration(sz*600) * time.Millisecond)
				t.Logf("done sleeping")

				verifyState := func(replaceLeader bool) {
					if len(members) == 0 {
						t.Logf("members was empty, returning early")
						return
					}

					var found *cluster.Node
					for _, member := range members {
						if leader := member.Leader(); leader != nil {
							found = leader
							break
						}
					}
					if found == nil {
						t.Fatalf("No leader found on any of the cluster nodes, is zookeeper even running?")
					}

					expectedLeaderStr := found.String()
					allMatch := true

					for i, member := range members {
						var leaderStr string
						if leader := member.Leader(); leader != nil {
							leaderStr = member.Leader().String()
						}
						t.Logf("%s thinks the leader is=/%s/", member.Id(), leaderStr)
						if leaderStr != expectedLeaderStr {
							t.Errorf("%s had leader=/%s/ but expected value=/%s/, caused allMatch=false", member.Id(), leaderStr, expectedLeaderStr)
							allMatch = false
						}

						if replaceLeader && member.Mode() == cluster.Leader {
							if err := member.Stop(); err != nil {
								t.Fatal(err)
							}
							members[i] = ncc(t, zkServers)
							t.Logf("Shut down leader member=%s and launched new one=%s", member.Id(), members[i].Id())
						}
					}

					if !allMatch {
						t.Fatalf("not all cluster coordinators agreed on who the leader was")
					}
				}

				for i := 0; i < sz*2; i++ {
					t.Logf("iteration #%v tc_sz=%v members_sz=%v [ mutate ]----------------", i, len(zkServers), sz)
					verifyState(true)

					time.Sleep(100 * time.Millisecond)
					t.Logf("iteration #%v tc_sz=%v members_sz=%v [ verify ]----------------", i, len(zkServers), sz)
					verifyState(false)
				}

				for _, member := range members {
					if err := member.Stop(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func Test_ClusterSubscriptions(t *testing.T) {
	testutil.WithTestZkCluster(t, 1, func(zkServers []string) {
		var (
			subChan           = make(chan cluster.Update)
			lock              sync.Mutex
			numEventsReceived int
		)

		go func() {
			for {
				select {
				case updateInfo := <-subChan:
					log.Info("New update=%+v", updateInfo)
					lock.Lock()
					numEventsReceived++
					lock.Unlock()
				}
			}
		}()

		cc := ncc(t, zkServers, subChan)

		defer func() {
			if err := cc.Stop(); err != nil {
				t.Fatal(err)
			}
		}()

		time.Sleep(1 * time.Second)

		cc.Unsubscribe(subChan)

		if err := cc.Stop(); err != nil {
			t.Fatal(err)
		}

		lock.Lock()
		if numEventsReceived < 1 {
			t.Fatalf("Expected numEventsReceived >= 1, but actual numEventsReceived=%v", numEventsReceived)
		}
		prevNumEventsReceived := numEventsReceived
		lock.Unlock()

		if err := cc.Start(); err != nil {
			t.Fatal(err)
		}

		// Verify that unsubscribe works.

		time.Sleep(1 * time.Second)

		lock.Lock()
		if numEventsReceived != prevNumEventsReceived {
			t.Fatalf("Expected numEventsReceived to stay the same (was previously %v), but actual numEventsReceived=%v", prevNumEventsReceived, numEventsReceived)
		}
		lock.Unlock()
	})
}

func TestClusterMembersListing(t *testing.T) {
	for _, n := range []int{1, 2, 3, 5, 7, 11} {
		testutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
			var (
				ccs             = []*cluster.Coordinator{}
				ready           = make(chan struct{})
				signalWhenReady = func(ch chan cluster.Update) {
					select {
					case <-ch:
						ready <- struct{}{}
					case <-time.After(zkTimeout):
						t.Fatalf("Timed out after %v waiting for ready signal", zkTimeout)
					}
				}
			)

			for i := 0; i < n; i++ {
				subChan := make(chan cluster.Update)
				go signalWhenReady(subChan)
				cc := ncc(t, zkServers, subChan)
				<-ready
				ccs = append(ccs, cc)
				for j := 0; j < len(ccs); j++ {
					nodes, err := ccs[j].Members()
					if err != nil {
						t.Fatalf("[i=%v j=%v] %s", i, j, err)
					}
					if expected, actual := i+1, len(nodes); actual != expected {
						t.Fatal("[i=%v j=%v] Expected number of members=%v but actual=%v; returned nodes=%+v", i, j, expected, actual, nodes)
					}
				}
			}

			for i := n - 1; i >= 0; i-- {
				if err := ccs[i].Stop(); err != nil {
					t.Fatal("[i=%v] %s", err)
				}
				ccs = ccs[0 : len(ccs)-1]
				for j := 0; j < len(ccs); j++ {
					nodes, err := ccs[j].Members()
					if err != nil {
						t.Fatalf("[i=%v j=%v] %s", i, j, err)
					}
					if expected, actual := i, len(nodes); actual != expected {
						t.Fatal("[i=%v j=%v] Expected number of members=%v but actual=%v; returned nodes=%+v", i, j, expected, actual, nodes)
					}
				}
			}
		})
	}
}
