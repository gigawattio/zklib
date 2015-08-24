package cluster

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// ncc creates a new Coordinator for a given test cluster.
func ncc(t *testing.T, tc *zk.TestCluster, subscribers ...chan Update) *Coordinator {
	servers := make([]string, len(tc.Servers))
	for i := 0; i < len(servers); i++ {
		servers[i] = fmt.Sprintf("localhost:%v", tc.Servers[i].Port)
	}
	cc, err := NewCoordinator(servers, 1*time.Second, "/comorgnet/election", subscribers...)
	if err != nil {
		t.Fatal(err)
	}
	if err := cc.Start(); err != nil {
		t.Fatal(err)
	}
	return cc
}

func Test_ClusterLeaderElection(t *testing.T) {
	// NB: tcSz == zookeeper test cluster size.
	for _, tcSz := range []int{1} {
		tc, err := zk.StartTestCluster(tcSz, os.Stdout, os.Stderr)
		if err != nil {
			t.Fatal(err)
		}
		defer func(tc *zk.TestCluster) { tc.Stop() }(tc)

		for _, sz := range []int{1, 2, 3, 4} {
			t.Logf("Testing with number of cluster members sz=%v", sz)

			members := make([]*Coordinator, sz)
			for i := 0; i < len(members); i++ {
				cc := ncc(t, tc)
				members[i] = cc

				go func() {
					wait := rand.Intn(2000)
					t.Logf("random wait for member=%s --> %vms", cc.Id(), wait)
					time.Sleep(time.Duration(wait) * time.Millisecond)
				}()
			}

			time.Sleep(2000 * time.Millisecond)
			t.Logf("done sleeping")

			verifyState := func(replaceLeader bool) {
				if len(members) == 0 {
					t.Logf("members was empty, returning early")
					return
				}

				var found *Node
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

					if replaceLeader && member.Mode() == Leader {
						if err := member.Stop(); err != nil {
							t.Fatal(err)
						}
						members[i] = ncc(t, tc)
						t.Logf("Shut down leader member=%s and launched new one=%s", member.Id(), members[i].Id())
					}
				}

				if !allMatch {
					t.Fatalf("not all cluster coordinators agreed on who the leader was")
				}
			}

			for i := 0; i < sz*2; i++ {
				t.Logf("iteration #%v tc_sz=%v members_sz=%v [ mutate ]----------------", i, len(tc.Servers), sz)
				verifyState(true)

				time.Sleep(100 * time.Millisecond)
				t.Logf("iteration #%v tc_sz=%v members_sz=%v [ verify ]----------------", i, len(tc.Servers), sz)
				verifyState(false)
			}

			for _, member := range members {
				if err := member.Stop(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tc.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func Test_ClusterSubscriptions(t *testing.T) {
	tc, err := zk.StartTestCluster(1, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer func(tc *zk.TestCluster) { tc.Stop() }(tc)

	var lock sync.Mutex
	numEventsReceived := 0

	subChan := make(chan Update)
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

	cc := ncc(t, tc, subChan)

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

	if err := tc.Stop(); err != nil {
		t.Fatal(err)
	}
}
