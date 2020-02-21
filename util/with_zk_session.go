package util

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

// WithZkSession invokes the invoker-supplied callback function once the
// zookeeper connection is established.
func WithZkSession(zkServers []string, zkTimeout time.Duration, callbackFunc func(conn *zk.Conn) error) error {
	conn, eventCh, err := zk.Connect(zkServers, zkTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		select {
		case ev := <-eventCh: // Watch connection events.
			if ev.Err != nil {
				return fmt.Errorf("eventCh: %s", err)
			}
			log.Debugf("eventCh: received event=%+v", ev)
			if ev.Type == zk.EventSession {
				switch ev.State {
				case zk.StateHasSession:
					goto InvokeCallback
				}
			}
		case <-time.After(zkTimeout):
			return fmt.Errorf("timed out after %v waiting for zk to connect", zkTimeout)
		}
	}
InvokeCallback:
	if err := callbackFunc(conn); err != nil {
		return err
	}
	return nil
}
