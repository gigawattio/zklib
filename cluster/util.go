package cluster

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
)

// retryUntilSuccess will keep attempting an operation until it succeeds.
//
// Exponential backoff is used to prevent failing attempts from looping madly.
func retryUntilSuccess(name string, operation func() error, strategy backoff.BackOff) {
	errNotifReceiver := func(err error, nextWait time.Duration) {
		log.Errorf("%s notified of error: %s [next wait=%s]", name, err, nextWait)
	}
	for {
		if err := backoff.RetryNotify(operation, strategy, errNotifReceiver); err != nil {
			log.Errorf("%s failure: %s [will keep trying]", name, err)
			strategy.Reset()
			continue
		}
		break
	}
}
