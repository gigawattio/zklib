package candidate_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/gigawattio/zklib/candidate"
	zktestutil "github.com/gigawattio/zklib/testutil"
	zkutil "github.com/gigawattio/zklib/util"
	"github.com/kr/pretty"
	"github.com/montanaflynn/stats"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

const (
	electionPath = "/candidate-test/election"
)

var (
	zkTimeout       = 5 * time.Second
	zkDeleteRetries = 10
)

func TestCandidateRegistration(t *testing.T) {
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		conn, zkEvents, err := zk.Connect(zkServers, zkTimeout)
		if err != nil {
			t.Fatal(err)
		}

		zktestutil.WhenZkHasSession(zkEvents, func() {
			if err := zkutil.RecursivelyDelete(conn, electionPath, zkDeleteRetries); err != nil {
				t.Fatal(err)
			}

			var (
				uid       = uuid.Must(uuid.NewV4())
				node      = candidate.NewNode(uid.String(), "127.0.0.1", 8000, nil)
				candidate = candidate.New(electionPath, node)
			)

			leaderChan, err := candidate.Register(conn)
			if err != nil {
				t.Fatal(err)
			}
			if leaderChan == nil {
				t.Fatalf("Candidate.Register() returned an invalid set of values: nil leaderChan and nil error")
			}

			if err := candidate.Unregister(); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func TestCandidateLeadershipRetention(t *testing.T) {
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		iterations := 5
		for _, n := range []int{1, 2, 3, 5, 7} {
			for i := 0; i < iterations; i++ {
				log.Infof("Starting leadership retention test iteration #%v/%v for n=%v", i+1, iterations, n)
				t.Logf("Starting leadership retention test iteration #%v/%v for n=%v", i+1, iterations, n)
				candidateConsensusTestCase(t, n, true, zkServers)
				if t.Failed() {
					t.FailNow()
				}
			}
		}
	})
}

func TestCandidateConsensus(t *testing.T) {
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		candidateConsensusTestCase(t, 3, false, zkServers)
	})
}

func TestCandidateConsensusRigorously(t *testing.T) {
	zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
		var (
			iterations = 10
			n          = 3
		)

		statistics := func(iterations, n int, attemptsData []float64, durationData []float64) {
			var (
				attempts  = map[string]float64{}
				durations = map[string]time.Duration{}
				f         float64
				err       error
			)

			if attempts["min"], err = stats.Min(attemptsData); err != nil {
				t.Fatal(err)
			}
			if attempts["max"], err = stats.Max(attemptsData); err != nil {
				t.Fatal(err)
			}
			if attempts["mean"], err = stats.Mean(attemptsData); err != nil {
				t.Fatal(err)
			}
			if attempts["median"], err = stats.Median(attemptsData); err != nil {
				t.Fatal(err)
			}

			t.Logf("Attempts statistics for iterations=%v n=%v: %+v", iterations, n, attempts)

			if f, err = stats.Min(durationData); err != nil {
				t.Fatal(err)
			}
			durations["min"] = time.Duration(int64(f))
			if f, err = stats.Max(durationData); err != nil {
				t.Fatal(err)
			}
			durations["max"] = time.Duration(int64(f))
			if f, err = stats.Mean(durationData); err != nil {
				t.Fatal(err)
			}
			durations["mean"] = time.Duration(int64(f))
			if f, err = stats.Median(durationData); err != nil {
				t.Fatal(err)
			}
			durations["median"] = time.Duration(int64(f))

			t.Logf("Duration statistics for iterations=%v n=%v: %+v", iterations, n, durations)
		}

		run := func(iterations, n int) {
			if t.Failed() {
				return
			}

			var (
				attemptsData = make([]float64, iterations)
				durationData = make([]float64, iterations)
			)

			defer func() {
				statistics(iterations, n, attemptsData, durationData)
			}()

			for i := 0; i < iterations; i++ {
				log.Infof("Starting rigorous consensus test iteration #%v/%v with n=%v", i+1, iterations, n)
				t.Logf("Starting rigorous consensus test iteration #%v/%v with n=%v", i+1, iterations, n)
				attempts, duration := candidateConsensusTestCase(t, n, false, zkServers)
				if t.Failed() {
					return
				}
				attemptsData = append(attemptsData, float64(attempts))
				durationData = append(durationData, float64(duration.Nanoseconds()))
			}
		}

		iterations = 100

		run(iterations, n)

		n = 5

		run(iterations, n)

		n = 7

		if testing.Short() {
			log.Warnf("Shortening test run time: skipping run with iterations=%v n=%v", iterations, n)
		} else {
			run(iterations, n)
		}

		iterations = 250

		n = 3

		if testing.Short() {
			log.Warnf("Shortening test run time: skipping run with iterations=%v n=%v", iterations, n)
		} else {
			run(iterations, n)
		}
	})
}

// candidateConsensusTestCase note: when firstCandidateShouldLead=true execution
// time increases due to the wait for first candidate to obtain leader.
func candidateConsensusTestCase(t *testing.T, n int, firstCandidateShouldLead bool, zkServers []string) (attempts int, duration time.Duration) {
	const consensusZkTimeout = 25 * time.Millisecond

	var (
		conns       = make([]*zk.Conn, 0, n)
		candidates  = make([]*candidate.Candidate, 0, n)
		leaderChans = make([]<-chan *candidate.Node, 0, n)
		deferreds   = []func(){}
	)

	defer func() {
		for _, deferred := range deferreds {
			deferred()
		}
	}()

	var (
		// zeroLeader seeds `leaders' since we consume the first leaders
		// update event when firstCandidateShouldLead=true.
		zeroLeader *candidate.Node
		randSeed   int64
	)

	for i := 0; i < n; i++ {
		conn, zkEvents, err := zk.Connect(zkServers, consensusZkTimeout)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		deferreds = append(deferreds, conn.Close)

		zktestutil.WhenZkHasSession(zkEvents, func() {
			if i == 0 {
				if err := zkutil.RecursivelyDelete(conn, electionPath, zkDeleteRetries); err != nil {
					t.Fatalf("Recursively deleting path=%v: %s", electionPath, err)
				}
			}

			var (
				node      = candidate.NewNode(fmt.Sprintf("%v", i), "127.0.0.1", 8000+i, nil)
				candidate = candidate.New(electionPath, node)
			)

			leaderChan, err := candidate.Register(conn)
			if err != nil {
				t.Fatal(err)
			}
			leaderChans = append(leaderChans, leaderChan)
			candidates = append(candidates, candidate)
			if firstCandidateShouldLead && i == 0 {
				// Ensures the first node obtains leadership status so we can later verify
				// that it has retained leadership status.
				zeroLeader = <-leaderChan
			}
		})
	}

	var (
		// 1st goroutine vars.
		updateReceivedFrom = map[int]struct{}{} // map[candidate-index]<struct-is-ignored>.
		heardFromEveryone  = make(chan struct{}, 1)

		// 2nd goroutine vars.
		leadersSignals = make(chan int, n*n)
		leaders        = map[int]*candidate.Node{} // map[candidate-index]*Node.
		leadersLock    sync.Mutex
	)

	if firstCandidateShouldLead {
		updateReceivedFrom[0] = struct{}{}
		leaders[0] = zeroLeader

		if n >= 2 {
			// Unregister and re-register all candidates but the first in a
			// randomized order.
			randSeed = time.Now().UnixNano()
			log.Infof("randSeed=%v", randSeed)
			t.Logf("randSeed=%v", randSeed)
			rand.Seed(randSeed)

			var (
				err             error
				leaderChan      <-chan *candidate.Node
				numReregistered int
			)

			for numReregistered < n-2 {
				index := rand.Intn(n)
				for index == 0 {
					index = rand.Intn(n)
				}
				if err = candidates[index].Unregister(); err != nil {
					t.Fatal(err)
				}
				if leaderChan, err = candidates[index].Register(conns[index]); err != nil {
					t.Fatal(err)
				}
				leaderChans[index] = leaderChan
				time.Sleep(5 * time.Millisecond)
				log.Infof("re-registered candidates[%v]", index)
				numReregistered++
			}
		}
	}

	// This goroutine triggers "heardFromEveryone" signal when it's heard
	// of at least 1 update from each coordinator.
	go func() {
		for {
			if len(updateReceivedFrom) == n {
				log.Debugf("Sending signal to heardFromEveryone")
				t.Log("Sending signal to heardFromEveryone")
				heardFromEveryone <- struct{}{}
				return
			}
			i, ok := <-leadersSignals
			if !ok {
				return
			}
			updateReceivedFrom[i] = struct{}{}
		}
	}()

	go func() {
		for i := 0; i < len(leaderChans); i++ {
			go func(i int) {
				for {
					select {
					case leader, ok := <-leaderChans[i]:
						if !ok {
							t.Logf("Detected closure of leaderChan[%v], loop exiting", i)
							return
						}

						t.Logf("New leader received from candidate with uuid=%v: %+v", candidates[i].Node.Uuid, *leader)

						leadersLock.Lock()
						leaders[i] = leader
						leadersLock.Unlock()

						select {
						case leadersSignals <- i:
						default:
							log.Warnf("Warning: Signal tick not received for leaderChan transmitter i=%v", i)
							t.Logf("Warning: Signal tick not received for leaderChan transmitter i=%v", i)
						}
					}
				}
			}(i)
		}
	}()

	// conns[0].Close()
	// conns[1].Close()

	verifyState := func() (buf *bytes.Buffer, err error) {
		buf = &bytes.Buffer{}

		leadersLock.Lock()
		defer leadersLock.Unlock()

		if actual, expected := len(leaders), n; actual != expected {
			buf.WriteString(fmt.Sprintf("leaders=%+v\n", leaders))
			err = fmt.Errorf("Expected num leaders=%v but actual=%v", expected, actual)
			return
		}
		for i, leader := range leaders {
			if leader := leaders[i]; leader == nil {
				err = fmt.Errorf("nil leader detected; leaders[i]=%v", leader)
				return
			}
			buf.WriteString(fmt.Sprintf("leaders[%v]=%+v\n", i, *leader))
		}
		if firstCandidateShouldLead {
			for i := 0; i < n; i++ {
				if actual, expected := fmt.Sprintf("%+v", *leaders[i]), fmt.Sprintf("%+v", *candidates[0].Node); actual != expected {
					err = fmt.Errorf("First candidate didn't lead:\nExpected candidates[0].Node=leaders[%v]=%+v but actual=%+v", i, expected, actual)
					return
				}
			}
		} else {
			for i := 1; i < n; i++ {
				if actual, expected := fmt.Sprintf("%+v", *leaders[i]), fmt.Sprintf("%+v", *leaders[0]); actual != expected {
					err = fmt.Errorf("Candidate leader didn't match:\nExpected candidates[0].Node=leaders[%v]=%+v but actual=%+v", i, expected, actual)
					return
				}
			}
		}
		return
	}

	startedAt := time.Now()

	const (
		heardFromEveryoneTimeout = 15 * time.Second
		maxAttempts              = 100
	)

	select {
	case <-heardFromEveryone:

	case <-time.After(heardFromEveryoneTimeout):
		close(leadersSignals)
		t.Logf("updateReceivedFrom=%+v", updateReceivedFrom)
		t.Fatalf("Timed out after %s waiting to hear at least 1 update from each candidate", heardFromEveryoneTimeout)
	}

	for attempts = 1; attempts <= maxAttempts; attempts++ {
		buf, err := verifyState()
		if err != nil {
			if attempts < maxAttempts-1 {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			t.Logf("Verification buffer output: %s", buf.String())
			duration = time.Now().Sub(startedAt)
			t.Fatalf("Verification failed after %v attempts: %s (n=%v)", attempts+1, err, n)
		}
		t.Logf("Verification buffer output: %s", buf.String())
		duration = time.Now().Sub(startedAt)
		t.Logf("Verification succeeded after %v tries over the course of %s (n=%v)", attempts+1, duration, n)
		break
	}

	for _, candidate := range candidates {
		if err := candidate.Unregister(); err != nil {
			t.Errorf("Unregistering candidate with uuid=%v produced err=%s (n=%v)", candidate.Node.Uuid, err, n)
		}
	}

	return
}

func TestCandidateParticipants(t *testing.T) {
	const participantsZkTimeout = 25 * time.Millisecond

	for _, n := range []int{1, 3, 5, 7} {
		zktestutil.WithZk(t, 1, "127.0.0.1:2181", func(zkServers []string) {
			conn, zkEvents, err := zk.Connect(zkServers, participantsZkTimeout)
			if err != nil {
				t.Fatalf("Error connecting to ZooKeeper: %s (n=%v)", err, n)
			}
			defer conn.Close()

			zktestutil.WhenZkHasSession(zkEvents, func() {
				if err := zkutil.RecursivelyDelete(conn, electionPath, zkDeleteRetries); err != nil {
					t.Fatal(err)
				}

				var (
					candidates  = make([]*candidate.Candidate, 0, n)
					leaderChans = make([]<-chan *candidate.Node, 0, n)
				)

				for i := 0; i < n; i++ {
					var (
						uid        = uuid.Must(uuid.NewV4())
						node       = candidate.NewNode(uid.String(), "127.0.0.1", 8000+i, nil)
						c          = candidate.New(electionPath, node)
						leaderChan <-chan *candidate.Node
						err        error
					)

					if leaderChan, err = c.Register(conn); err != nil {
						t.Fatal(err)
					}
					defer func(i int, c *candidate.Candidate) {
						if err := c.Unregister(); err != nil {
							t.Errorf("Error unregistering candidate=%+v: %s (i=%v, n=%v)", *c, err, i, n)
						}
					}(i, c)
					candidates = append(candidates, c)
					leaderChans = append(leaderChans, leaderChan)
				}
				if t.Failed() {
					t.FailNow()
				}

				for _, leaderChan := range leaderChans {
					<-leaderChan
				}

				for i := 0; i < n; i++ {
					participants, err := candidates[i].Participants(conn)
					if err != nil {
						t.Errorf("Error getting participants from candidates[%v]: %s (n=%v)", i, err, n)
					}
					if actual, expected := len(participants), n; actual != expected {
						t.Logf("Participants for candidates[%v]: %# v (n=%v)", i, pretty.Formatter(participants), n)
						t.Errorf("Expected number of participants=%v but actual=%v for candidates[%v] (n=%v)", expected, actual, i, n)
					}
				}
			})
		})
		if t.Failed() {
			t.FailNow()
		}
	}
}
