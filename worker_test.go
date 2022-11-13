package pgqueue

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func Test_worker_processTask(t *testing.T) {

	someTask := &Task{
		ID:      123,
		Kind:    456,
		Payload: nil,
	}

	testCases := []struct {
		name             string
		handler          TaskHandler
		attemptsLeft     uint16
		attemptsCount    uint16
		delay            uint32
		progressiveDelay []uint32
		expectDelay      uint32
		expectRefuse     bool
		expectClose      bool
		expectComplete   bool
	}{
		{
			name:         "usual error, refuse",
			handler:      newErrorHandler(errForTest),
			expectRefuse: true,
		},
		{
			name:          "usual error, refuse with delay",
			handler:       newErrorHandler(errForTest),
			delay:         10,
			expectDelay:   10,
			expectRefuse:  true,
			attemptsLeft:  2,
			attemptsCount: 4,
		},
		{
			name:             "usual error, refuse with progressive delay, first attempt",
			handler:          newErrorHandler(errForTest),
			progressiveDelay: []uint32{60, 300},
			expectDelay:      60,
			expectRefuse:     true,
			attemptsLeft:     3,
			attemptsCount:    4,
		},
		{
			name:             "usual error, refuse with progressive delay, attempts equal delays len",
			handler:          newErrorHandler(errForTest),
			progressiveDelay: []uint32{60, 300},
			expectDelay:      300,
			expectRefuse:     true,
			attemptsLeft:     2,
			attemptsCount:    4,
		},
		{
			name:             "usual error, refuse with progressive delay, last attempt",
			handler:          newErrorHandler(errForTest),
			progressiveDelay: []uint32{60, 300},
			expectDelay:      300,
			expectRefuse:     true,
			attemptsLeft:     1,
			attemptsCount:    4,
		},
		{
			name: "special error, close",
			handler: newErrorHandler(
				errors.WithStack(
					errors.Wrap(
						errors.Wrapf(
							ErrMustAbortTask, // wrapped many times
							"randomInt=%d", 42,
						),
						"ggg",
					),
				),
			),
			expectClose: true,
		},
		{
			name:           "nil, complete",
			handler:        okHandler(0),
			expectComplete: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			store := NewStorageMock(t)
			workerMonitor := NewWorkerMonitorMock(t)
			metricsCollector := NewMetricsCollectorMock(t)
			metricsCollector.IncBusyWorkersMock.Expect(processorNameForTest, someTask.Kind)
			metricsCollector.DecBusyWorkersMock.Expect(processorNameForTest, someTask.Kind)
			metricsCollector.MeasureTaskDurationMock.Inspect(
				func(processorName string, taskKind int16, isSuccessful bool, duration time.Duration) {
					require.Equal(t, processorNameForTest, processorName)
					require.Equal(t, someTask.Kind, taskKind)
					if tc.expectComplete {
						require.True(t, isSuccessful)
					} else {
						require.False(t, isSuccessful)
					}
				}).Return()

			someTask.attemptsLeft = tc.attemptsLeft

			if tc.expectRefuse {
				store.refuseTaskMock.Set(func(ctx context.Context, id int64, reason string, delaySeconds uint32) (err error) {
					if delaySeconds != tc.expectDelay {
						t.Fatalf("unexpexted delay, expected: %d, actual: %d", tc.expectDelay, delaySeconds)
					}
					return nil
				})
			}
			if tc.expectClose {
				store.abortTaskMock.Set(func(ctx context.Context, id int64, reason string) (err error) {
					return nil
				})
			}
			if tc.expectComplete {
				store.completeTaskMock.Set(func(ctx context.Context, id int64) (err error) {
					return nil
				})
			}

			w := newWorker(
				0,
				processorNameForTest,
				"",
				store,
				workerMonitor,
				nil,
				tc.attemptsCount,
				1,
				tc.delay,
				tc.progressiveDelay,
				metricsCollector,
			)

			w.processTask(ctx, tc.handler, someTask)
		})
	}
}

const processorNameForTest = "some processor name"
