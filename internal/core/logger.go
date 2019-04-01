package core

import (
	"log"
	"os"
	"runtime/debug"
	"strings"
)

// ConfigLogger is the key for the pipeline's logger
const ConfigLogger = "Core.Logger"

// Logger defines the output interface used by Hercules components.
type Logger interface {
	Info(...interface{})
	Infof(string, ...interface{})
	Warn(...interface{})
	Warnf(string, ...interface{})
	Error(...interface{})
	Errorf(string, ...interface{})
}

// DefaultLogger is the default logger used by a pipeline, and wraps the standard
// log library.
type DefaultLogger struct {
	I *log.Logger
	W *log.Logger
	E *log.Logger
}

// NewLogger returns a configured default logger.
func NewLogger() *DefaultLogger {
	return &DefaultLogger{
		I: log.New(os.Stdout, "[INFO] ", log.LstdFlags),
		W: log.New(os.Stdout, "[WARN] ", log.LstdFlags),
		E: log.New(os.Stderr, "[ERROR] ", log.LstdFlags),
	}
}

// Info writes to info logger
func (d *DefaultLogger) Info(v ...interface{}) { d.I.Print(v...) }

// Infof writes to info logger
func (d *DefaultLogger) Infof(f string, v ...interface{}) { d.I.Printf(f, v...) }

// Warn writes to the warning logger
func (d *DefaultLogger) Warn(v ...interface{}) { d.W.Print(v...) }

// Warnf writes to the warning logger
func (d *DefaultLogger) Warnf(f string, v ...interface{}) { d.W.Printf(f, v...) }

// Error writes to the error logger
func (d *DefaultLogger) Error(v ...interface{}) {
	d.E.Print(v...)
	d.logStacktraceToErr()
}

// Errorf writes to the error logger
func (d *DefaultLogger) Errorf(f string, v ...interface{}) {
	d.E.Printf(f, v...)
	d.logStacktraceToErr()
}

// logStacktraceToErr prints a stacktrace to the logger's error output.
// It skips 4 levels that aren't meaningful to a logged stacktrace:
// * debug.Stack()
// * core.captureStacktrace()
// * DefaultLogger::logStacktraceToErr()
// * DefaultLogger::Error() or DefaultLogger::Errorf()
func (d *DefaultLogger) logStacktraceToErr() {
	d.E.Println("stacktrace:\n" + strings.Join(captureStacktrace(0), "\n"))
}

func captureStacktrace(skip int) []string {
	stack := string(debug.Stack())
	lines := strings.Split(stack, "\n")
	return lines[2*skip+1:]
}
