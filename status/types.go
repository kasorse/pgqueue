package status

// Status represents the status of a task.
type Status int64

// Possible task statuses.
const (
	// OpenNew is assigned to the task when it is created.
	OpenNew Status = 0
	// OpenMustRetry is assigned to a task after a failed execution attempt,
	// if the attempt limit has not yet been exhausted.
	OpenMustRetry Status = 1
	// OpenProcessing means that the task is currently being executed by the worker.
	OpenProcessing Status = 2

	// OpenClosedStatusesDivide - value separating Open/Closed statuses. Needed to
	// check status < OpenClosedStatusesDivide If necessary, you can change, but
	// you can not allow intersections with statuses.
	OpenClosedStatusesDivide = 3

	// ClosedCancelled means that the task was canceled by the user.
	ClosedCancelled Status = 95
	// ClosedAborted assigned to a task if the custom handler returned a
	// pgqueue.ErrMustAbortTask error.
	ClosedAborted Status = 96
	// ClosedLost is assigned to a task if the next attempt to execute it failed to
	// complete within AttemptLimitSeconds seconds, and attempts have ended after that.
	ClosedLost Status = 97
	// ClosedExpired means that the task was forcibly terminated because it could
	// not complete within TTLSeconds after being created.
	ClosedExpired Status = 98
	// ClosedNoAttemptsLeft means that the task failed the maximum number of times allowed
	ClosedNoAttemptsLeft Status = 99
	// ClosedSuccess means the task completed successfully.
	ClosedSuccess Status = 100
)

// Desc returns a string representation of the status.
func (s Status) Desc() string {
	return getName(s)
}

var (
	description = map[Status]string{
		OpenNew:        "new",
		OpenMustRetry:  "must_retry",
		OpenProcessing: "processing",

		ClosedCancelled:      "cancelled",
		ClosedAborted:        "aborted",
		ClosedLost:           "lost",
		ClosedExpired:        "expired",
		ClosedNoAttemptsLeft: "no_attempts",
		ClosedSuccess:        "success",
	}
)

func getName(st Status) string {
	desc, ok := description[st]
	if !ok {
		desc = "unknown"
	}
	return desc
}
