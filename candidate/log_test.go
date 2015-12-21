package candidate_test

import (
	"os"
	"testing"

	"github.com/op/go-logging"
)

const PACKAGE = "candidate_test"

var log = logging.MustGetLogger(PACKAGE)

type inlineTestWriter struct {
	t *testing.T
}

func (w *inlineTestWriter) Write(bs []byte) (n int, err error) {
	w.t.Logf("%s", string(bs))
	n, err = os.Stdout.Write(bs)
	return
}

func InitTestLogging(t *testing.T) {
	// logFormat is the log format string. Everything except the message has a
	// custom color which is dependent on the log level. Many fields have a
	// custom output formatting too, e.g. the temporal resolution is to the
	// millisecond.
	format := logging.MustStringFormatter(
		"%{color}%{time:2006-01-02 15:04:05.000} %{id:03x} %{shortpkg} %{shortfile} %{shortfunc} ▶ %{level:.4s} %{color:reset} %{message}",
	)

	writer := &inlineTestWriter{
		t: t,
	}
	backend := logging.NewLogBackend(writer, "", 0)

	// For messages written to backend we add some additional information to the
	// output, including the used log level and the name of the function.
	backendFormatter := logging.NewBackendFormatter(backend, format)

	// Set the backend(s) to be used.
	log.SetBackend(logging.AddModuleLevel(backendFormatter))
}
