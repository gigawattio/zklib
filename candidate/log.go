package candidate

import (
	"gigawatt-common/pkg/logginglib"

	"github.com/op/go-logging"
)

const PACKAGE = "candidate"

var log = logging.MustGetLogger(PACKAGE)

func init() {
	logginglib.InitializeFormat()
}
