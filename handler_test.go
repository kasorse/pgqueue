package pgqueue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/kasorse/pgqueue/internal/logger"
	"github.com/stretchr/testify/assert"
)

const maxAttempts = 3

type TaskID int64 // used only for testing purposes

type testPayload struct {
	TaskID TaskID `json:"task_id"`
}

func (td TaskID) toPayload() []byte {
	b, err := json.Marshal(&testPayload{TaskID: td})
	if err != nil {
		panic(err)
	}
	return b
}

type testHandler struct {
	t         *testing.T
	responses map[TaskID][]error
	positions map[TaskID]int64
	mu        *sync.Mutex
}

func newHandler(t *testing.T, responses map[TaskID][]error) *testHandler {
	positions := make(map[TaskID]int64)
	for k := range responses {
		positions[k] = 0
	}
	return &testHandler{
		t:         t,
		responses: responses,
		positions: positions,
		mu:        &sync.Mutex{},
	}
}

func (th *testHandler) checkAttemptsCount() {
	th.mu.Lock()
	defer th.mu.Unlock()
	for taskID, series := range th.responses {
		pos, ok := th.positions[taskID]
		if !ok {
			th.t.Fatal("series not found by position")
		}
		assert.Equalf(th.t, int64(len(series)), pos, "wrong number of calls for %d", taskID)
	}
}

func (th *testHandler) HandleTask(ctx context.Context, task *Task) error {
	th.mu.Lock()
	defer th.mu.Unlock()

	if task == nil {
		th.t.Fatal("cannot continue test, bad task")
	}
	logger.Infof(context.Background(), "handle task with payload %v", task.Payload)

	payload := new(testPayload)
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		th.t.Fatal("cannot unmarshal test payload", err)
	}
	taskID := payload.TaskID
	errorSeries, ok := th.responses[taskID]
	if !ok {
		th.t.Fatal("unexpected task series")
	}

	pos, ok := th.positions[taskID]
	if !ok {
		th.t.Fatal("unexpected task position")
	}

	// pos also represents the number of (performed) attempts to handle a task
	if pos >= int64(len(errorSeries)) || pos >= maxAttempts {
		th.t.Fatal("len mismatch, this should not be called", pos, len(errorSeries), maxAttempts)
	}
	ret := errorSeries[pos]
	th.positions[taskID]++ // increment number of calls
	return ret
}
