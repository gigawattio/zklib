package testutil

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"gigawatt-common/pkg/concurrency"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	bindingToPortMsg    = " binding to port "
	addrAlreadyInUseMsg = "java.net.BindException: Address already in use"
)

var (
	maxStartRetries      = 5
	bindWaitTimeout      = 10 * time.Second
	portOpenWaitTimeout  = 5 * time.Second
	portCloseWaitTimeout = 5 * time.Second

	testZkClusterLock sync.Mutex
	testAlreadyUpLock sync.Mutex
	testAddrs         []string
)

type testLogger struct {
	sync.Mutex
	t               *testing.T
	prefix          string
	suffix          string
	bindNotifier    chan struct{}
	detectedFailure bool
}

func (logger *testLogger) Write(bs []byte) (n int, err error) {
	// TODO: Preserve the last 100 or 200 bytes to ensure the failure strings will always be detected.
	str := string(bs)
	// go func() {
	if strings.Contains(str, addrAlreadyInUseMsg) {
		logger.Lock()
		logger.detectedFailure = true
		logger.Unlock()
		logger.t.Logf("\033[1mDetected fatal error in ZooKeeper output: %v\033[0m", addrAlreadyInUseMsg)
	}
	if strings.Contains(str, bindingToPortMsg) {
		logger.bindNotifier <- struct{}{}
	}
	// }()
	logger.t.Logf("%v%v%v", logger.prefix, strings.Trim(str, "\n"), logger.suffix)
	n = len(bs)
	return
}
func (logger *testLogger) DetectedFailure() bool {
	logger.Lock()
	defer logger.Unlock()
	return logger.detectedFailure
}
func (logger *testLogger) Reset() *testLogger {
	logger.Lock()
	logger.detectedFailure = false
	logger.Unlock()
	return logger
}

func WithTestZkCluster(t *testing.T, size int, fn func(zkServers []string)) {
	// Check for an already-running cluster.
	testAlreadyUpLock.Lock()
	var alreadyRunningServers []string
	if len(testAddrs) > 0 {
		if l := len(testAddrs); l != size {
			testAlreadyUpLock.Unlock()
			t.Fatalf("Received incompatible ZK cluster size value=%v, already running a %v node cluster", size, l)
		}
		alreadyRunningServers = testAddrs
	}
	testAlreadyUpLock.Unlock()
	if len(alreadyRunningServers) > 0 {
		t.Logf("Will use already running ZooKeeper test cluster with addrs=%+v", alreadyRunningServers)
		fn(alreadyRunningServers)
		return
	} else {
		t.Logf("Launching fresh ZooKeeper test cluster")
	}

	var (
		stdout      = &testLogger{t: t, prefix: "\033[90m[stdout] ", suffix: "\033[0m", bindNotifier: make(chan struct{}, 1)}
		stderr      = &testLogger{t: t, prefix: "\033[90m[stderr] ", suffix: "\033[0m", bindNotifier: make(chan struct{}, 1)}
		zkServers   = make([]string, size)
		attempts    int
		shouldRetry = func() bool {
			if attempts < maxStartRetries && stdout.DetectedFailure() || stderr.DetectedFailure() {
				t.Logf("ZooKeeper start failure detected, will retry.. (attempts=%v/%v)", attempts, maxStartRetries)
				return true
			}
			return false
		}
	)

	testAlreadyUpLock.Lock()
	testZkClusterLock.Lock()
Retry:
	attempts++
	tc, err := zk.StartTestCluster(size, stdout.Reset(), stderr.Reset())
	if err != nil {
		testZkClusterLock.Unlock()
		testAlreadyUpLock.Unlock()
		t.Fatalf("Starting ZooKeeper test cluster: %s", err)
	}

	msgs := []string{fmt.Sprintf("Started a %v node ZooKeeper test cluster", len(tc.Servers))}

	for i, zkServer := range tc.Servers {
		zkServers[i] = fmt.Sprintf("127.0.0.1:%v", zkServer.Port)
		msgs = append(msgs, fmt.Sprintf("\tServer #%v listening on %v", i+1, zkServers[i]))
	}
	testAddrs = zkServers
	testAlreadyUpLock.Unlock()

	// Wait for binding to port.
	select {
	case <-stdout.bindNotifier:
		// pass
	case <-stderr.bindNotifier:
		// pass
	case <-time.After(bindWaitTimeout):
		testZkClusterLock.Unlock()
		t.Fatalf("Timed out after %s waiting for zk bind notification", bindWaitTimeout)
	}
	t.Log("Bind notification received")
	time.Sleep(10 * time.Millisecond)

	if shouldRetry() {
		goto Retry
	}
	if err := waitForPortsToOpen(zkServers, portOpenWaitTimeout); err != nil {
		if shouldRetry() {
			goto Retry
		}
		testZkClusterLock.Unlock()
		t.Fatalf("One or more open port checks failed: %s (start attempts=%v)", err, attempts)
	} else {
		for _, msg := range msgs {
			t.Log(msg)
		}
	}

	// time.Sleep(1 * time.Second)
	// // Verify it's still up.
	// if err := waitForPortsToOpen(zkServers, portOpenWaitTimeout); err != nil {
	// 	t.Fatalf("One or more open port checks failed: %s", err)
	// }

	fn(zkServers)

	defer testZkClusterLock.Unlock()

	if err := tc.Stop(); err != nil {
		t.Fatalf("Stopping ZooKeeper test cluster: %s", err)
	}

	if err := waitForPortsToClose(zkServers, portCloseWaitTimeout); err != nil {
		// Sometimes for whatever reason the processes don't get killed, hence the
		// double-tap.
		exec.Command("pkill", "-f", "java -jar .* server .*gozk.*").Run()
		if err := waitForPortsToClose(zkServers, portCloseWaitTimeout); err != nil {
			t.Fatalf("Waiting for ZooKeeper test ports to close: %s", err)
		}
	}
	// Wait a little while longer to ensure the port addresses will no longer be
	// in use.
	time.Sleep(portCloseWaitTimeout)
	if t.Failed() {
		t.FailNow()
	}

	// Clear out "running test node addresses" info.
	testAlreadyUpLock.Lock()
	testAddrs = nil
	testAlreadyUpLock.Unlock()
	t.Logf("Shut down ZooKeeper test cluster OK")

}

func WhenZkHasSession(zkEvents <-chan zk.Event, fn func()) {
	for {
		event := <-zkEvents
		switch event.Type {
		case zk.EventSession:
			switch event.State {
			case zk.StateHasSession:
				fn()
				return
			}
		}
	}
}

// waitForPortsToOpen verifies that the specified address-port pairs are all
// reachable.
func waitForPortsToOpen(addressPorts []string, timeout time.Duration) error {
	var (
		done          = make(chan error, 1)
		portChecks    = make([]func() error, 0, len(addressPorts))
		retryInterval = 5 * time.Millisecond
	)

	for _, addressPort := range addressPorts {
		func(addressPort string) {
			portCheck := func() error {
				for {
					cmd, err := portCheckCmd(addressPort)
					if err != nil {
						return err
					}
					output, err := cmd.CombinedOutput()
					if err := IsCommandNotFound(output, err); err != nil {
						return err
					}
					if err == nil { // Port is open!
						return nil
					}
					time.Sleep(retryInterval)
				}
			}
			portChecks = append(portChecks, portCheck)
		}(addressPort)
	}

	go func() {
		done <- concurrency.MultiGo(portChecks...)
	}()

	var err error

	select {
	case err = <-done:
	case <-time.After(timeout):
		err = fmt.Errorf("timed out after %s waiting for ports to open", timeout)
	}

	if err != nil {
		return err
	}
	return nil
}

// waitForPortsToClose waits until the specified address-port pairs are all
// unreachable.
func waitForPortsToClose(addressPorts []string, timeout time.Duration) error {
	var (
		done          = make(chan error, 1)
		waiters       = make([]func() error, 0, len(addressPorts))
		retryInterval = 5 * time.Millisecond
	)

	for _, addressPort := range addressPorts {
		func(addressPort string) {
			waiter := func() error {
				for {
					cmd, err := portCheckCmd(addressPort)
					if err != nil {
						return err
					}
					output, err := cmd.CombinedOutput()
					if err != nil {
						if err := IsCommandNotFound(output, err); err != nil {
							return err
						}
						return nil // Port is closed!
					}
					time.Sleep(retryInterval)
				}
			}
			waiters = append(waiters, waiter)
		}(addressPort)
	}

	go func() {
		done <- concurrency.MultiGo(waiters...)
	}()

	var err error

	select {
	case err = <-done:
	case <-time.After(timeout):
		err = fmt.Errorf("timed out after %s waiting for ports to close", timeout)
	}

	if err != nil {
		return err
	}
	return nil
}

func portCheckCmd(addressPort string) (*exec.Cmd, error) {
	if !strings.Contains(addressPort, ":") {
		return nil, fmt.Errorf("addressPort=%q has no colon (':') denoting a port for netcat", addressPort)
	}
	cmd := exec.Command("nc", append([]string{"-v", "-w", "1"}, strings.Split(addressPort, ":")...)...)
	return cmd, nil
}

// IsCommandNotFound attempts to determine if a cmd.CombinedOutput() result
// contains the text "command not found" and returns an error if so.
//
// Used to detect unrecoverable errors.
func IsCommandNotFound(output []byte, err error) error {
	if err != nil {
		outputStr := fmt.Sprintf("%v %v", err.Error(), string(output))
		if strings.Contains(outputStr, "command not found") || strings.Contains(outputStr, "executable file not found in $PATH") || strings.Contains(outputStr, "no such file or directory") {
			return fmt.Errorf("detected 'command not found' error: %s output=%q", err, outputStr)
		}
	}
	return nil
}
