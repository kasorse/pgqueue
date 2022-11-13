package pgqueue

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/pkg/errors"
)

type wrappedHandler struct {
	origin TaskHandler
}

func wrapHandler(handler TaskHandler) *wrappedHandler {
	return &wrappedHandler{origin: handler}
}

// HandleTask implements TaskHandler
func (wh *wrappedHandler) HandleTask(ctx context.Context, task *Task) error {
	trace, err := wh.handleWithRecover(ctx, task)
	if len(trace) > 0 {
		err = errors.Wrap(err, trace)
	}
	return err
}

// handleWithRecover returns task error and stack trace, if panic was recovered
func (wh *wrappedHandler) handleWithRecover(ctx context.Context, task *Task) (trace string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic in job: %v", p)
			trace = string(debug.Stack())
		}
	}()
	err = wh.origin.HandleTask(ctx, task)
	return
}
