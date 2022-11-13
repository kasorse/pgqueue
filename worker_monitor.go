package pgqueue

import (
	"sync"
)

type workerMon struct {
	totalCount uint16

	mu           sync.Mutex
	restingCount uint16
}

func newWorkerMonitor(
	workerCount uint16,
) *workerMon {
	return &workerMon{
		totalCount:   workerCount,
		restingCount: workerCount,
	}
}

func (wm *workerMon) getRestingCount() uint16 {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.restingCount
}

func (wm *workerMon) decreaseRestingCount() {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if wm.restingCount > 0 {
		wm.restingCount--
	}
}

func (wm *workerMon) increaseRestingCount() {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if wm.restingCount < wm.totalCount {
		wm.restingCount++
	}
}
