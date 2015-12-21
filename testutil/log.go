package testutil

import (
	"gigawatt-common/pkg/logginglib"

	"github.com/op/go-logging"
)

const PACKAGE = "testutil"

var log = logging.MustGetLogger(PACKAGE)

func init() {
	logginglib.InitializeFormat()
}
