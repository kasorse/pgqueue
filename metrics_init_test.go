package pgqueue

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// initKindCollector is a MetricsCollector that also implements KindInitializer and records its InitKind calls.
type initKindCollector struct {
	NoMetricsCollector

	mu    sync.Mutex
	calls []string
}

func (c *initKindCollector) InitKind(processorName string, taskKind int16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, fmt.Sprintf("%s/%d", processorName, taskKind))
}

func (c *initKindCollector) sortedCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.calls...)
	sort.Strings(out)
	return out
}

func newInitKindTestProcessor(t *testing.T, mc MetricsCollector) *processor {
	t.Helper()
	qp := &processor{kindData: make(map[int16]kindDescription), metricsCollector: mc}
	qp.RegisterKind(1, &TaskHandlerMock{}, Options{Name: "export"})
	qp.RegisterKind(4, &TaskHandlerMock{}, Options{Name: "full_import"})
	return qp
}

func TestInitKindMetrics_CallsEveryRegisteredKindOnce(t *testing.T) {
	c := &initKindCollector{}
	qp := newInitKindTestProcessor(t, c)

	qp.initKindMetrics()

	require.Equal(t, []string{"export/1", "full_import/4"}, c.sortedCalls())
}

func TestInitKindMetrics_CollectorWithoutInitKindIsLeftAlone(t *testing.T) {
	qp := newInitKindTestProcessor(t, &NoMetricsCollector{})

	require.NotPanics(t, qp.initKindMetrics)
}

// Start seeds the series before it returns, so they exist before any task of the kind can finish.
func TestStart_InitsKindsBeforeReturning(t *testing.T) {
	c := &initKindCollector{}
	qp := newInitKindTestProcessor(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the loops stop at once; only the initialisation is under test

	done, err := qp.Start(ctx)

	require.NoError(t, err)
	require.Equal(t, []string{"export/1", "full_import/4"}, c.sortedCalls())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processor loop did not stop after the context was cancelled")
	}
}
