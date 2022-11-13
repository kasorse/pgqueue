package pgqueue

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type panickingHandler int

func (panickingHandler) HandleTask(ctx context.Context, task *Task) error {
	panic("panic in handler")
}

func newErrorHandler(err error) errorHandler {
	return errorHandler{err: err}
}

var errForTest = errors.New("error in handler")

type errorHandler struct {
	err error
}

func (e errorHandler) HandleTask(ctx context.Context, task *Task) error {
	return e.err
}

type okHandler int

func (okHandler) HandleTask(ctx context.Context, task *Task) error {
	return nil
}

func TestWrappedHandler_handleWithRecover(t *testing.T) {
	a := assert.New(t)

	testCases := []struct {
		name        string
		handler     TaskHandler
		errCheck    func(error, ...interface{}) bool
		trace       string
		expectTrace bool
	}{
		{
			name:        "ok",
			handler:     okHandler(0),
			errCheck:    a.NoError,
			expectTrace: false,
		},
		{
			name:        "err",
			handler:     newErrorHandler(errForTest),
			errCheck:    a.Error,
			expectTrace: false,
		},
		{
			name:        "panic",
			handler:     panickingHandler(0),
			errCheck:    a.Error,
			expectTrace: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var task *Task
			wc := wrapHandler(tc.handler)

			trace, err := wc.handleWithRecover(ctx, task)
			tc.errCheck(err)

			if tc.expectTrace {
				a.True(len(trace) > 0)
			} else {
				a.Equal("", trace)
			}
		})
	}
}
