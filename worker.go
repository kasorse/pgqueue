package pgqueue

import (
	"context"
	"time"

	"github.com/kasorse/pgqueue/internal/logger"
	"github.com/pkg/errors"
)

type worker struct {
	id                                  uint16
	processorName                       string
	logPrologue                         string
	storage                             storage
	monitor                             workerMonitor
	tasks                               <-chan *Task
	attemptsCount                       uint16
	attemptLimitSeconds                 uint32
	delayAfterRefusedSeconds            uint32
	progressiveDelayAfterRefusedSeconds []uint32
	metricsCollector                    MetricsCollector
}

func newWorker(
	id uint16,
	processorName string,
	logPrologue string,
	storage storage,
	monitor workerMonitor,
	tasks <-chan *Task,
	attemptsCount uint16,
	attemptLimitSeconds uint32,
	delayAfterRefusedSeconds uint32,
	progressiveDelayAfterRefusedSeconds []uint32,
	metricsCollector MetricsCollector,
) *worker {
	return &worker{
		id:                                  id,
		processorName:                       processorName,
		logPrologue:                         logPrologue,
		storage:                             storage,
		monitor:                             monitor,
		tasks:                               tasks,
		attemptsCount:                       attemptsCount,
		attemptLimitSeconds:                 attemptLimitSeconds,
		delayAfterRefusedSeconds:            delayAfterRefusedSeconds,
		progressiveDelayAfterRefusedSeconds: progressiveDelayAfterRefusedSeconds,
		metricsCollector:                    metricsCollector,
	}
}

func (w *worker) run(
	ctx context.Context,
	handler TaskHandler,
) {
	logger.Infof(ctx, "%s [worker_id=%d] enter worker loop", w.logPrologue, w.id)
	defer logger.Infof(ctx, "%s [worker_id=%d] escape worker loop", w.logPrologue, w.id)

	for {
		select {
		case task := <-w.tasks:
			w.monitor.decreaseRestingCount()
			w.processTask(ctx, handler, task)
			w.monitor.increaseRestingCount()
		case <-ctx.Done():
			return
		}
	}
}

func (w *worker) processTask(
	parentCtx context.Context,
	handler TaskHandler,
	task *Task,
) {
	w.metricsCollector.IncBusyWorkers(w.processorName, task.Kind)
	defer w.metricsCollector.DecBusyWorkers(w.processorName, task.Kind)

	ctx := logger.WrapContext(parentCtx, w.logPrologue)
	logger.SetItems(ctx, map[string]interface{}{
		"worker_id": w.id,
		"task_id":   task.ID,
		"task_kind": task.Kind,
	})

	handlerContext, cancelHandler := context.WithTimeout(ctx, time.Duration(w.attemptLimitSeconds)*time.Second)
	defer cancelHandler()

	delay := w.delayAfterRefusedSeconds
	if len(w.progressiveDelayAfterRefusedSeconds) != 0 {
		attemptIndex := w.attemptsCount - task.attemptsLeft
		delayIndex := attemptIndex - 1
		delay = getDelayByAttemptIndex(w.progressiveDelayAfterRefusedSeconds, delayIndex)
	}

	if task.RepeatPeriod != 0 {
		delay = task.RepeatPeriod
	}

	startTime := time.Now()
	err := wrapHandler(handler).HandleTask(handlerContext, task)
	duration := time.Since(startTime)
	handleTimeMs := duration.Nanoseconds() / 1e6
	w.metricsCollector.MeasureTaskDuration(w.processorName, task.Kind, err == nil, duration)

	if err != nil {
		if errors.Cause(err) == ErrMustAbortTask {
			logger.Errorf(ctx, "must abort task, spent %d ms and failed with error: %v", handleTimeMs, err)
			abortErr := w.storage.abortTask(ctx, task.ID, err.Error())
			if abortErr != nil {
				logger.Errorf(ctx, "abortTask error: %v", abortErr)
			}
			return
		}

		if task.attemptsLeft > 0 {
			logger.Warnf(ctx, "has %d attempts, spent %d ms and failed with error: %v", task.attemptsLeft, handleTimeMs, err)
		} else {
			logger.Errorf(ctx, "no attempts left, spent %d ms and failed with error: %v", handleTimeMs, err)
		}

		refuseErr := w.storage.refuseTask(ctx, task.ID, err.Error(), delay)
		if refuseErr != nil {
			logger.Errorf(ctx, "refuseTask error: %v", refuseErr)
		}
	} else {
		logger.Infof(ctx, "spent %d ms and processed successfully", handleTimeMs)

		completeErr := w.storage.completeTask(ctx, task.ID, delay)
		if completeErr != nil {
			logger.Errorf(ctx, "completeTask error: %v", completeErr)
		}
	}
}
