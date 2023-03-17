package pgqueue

import (
	"time"

	"github.com/pkg/errors"
)

// default settings
const (
	defaultWorkerCount uint16 = 1
	defaultMaxAttempts uint16 = 1

	emptyKey         string        = ""
	zeroDuration     time.Duration = 0
	zeroRepeatPeriod uint32        = 0
)

var (
	// ErrMustAbortTask is returned by the custom task handler if the
	// task needs to be closed despite remaining retries.
	ErrMustAbortTask = errors.New("according to business logic task must be closed with no retries")

	// ErrUnexpectedTaskKind is returned by the processor when it tries
	// to enqueue a task of an unregistered type.
	ErrUnexpectedTaskKind = errors.New("unexpected task kind")
)

// AppendTaskOptions describes options for publishing a task.
type AppendTaskOptions struct {
	// The key that the task is associated with. Allows you to cancel tasks
	// by key. Is not unique.
	ExternalKey string
	// Delay in taking the task to work.
	Delay time.Duration
	// Period in seconds in which task will be retried
	RepeatPeriod uint32
}
