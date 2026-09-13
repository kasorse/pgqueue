package pgqueue

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kasorse/pgqueue/internal/logger"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	taskFetcherTickerInterval     = 1111 * time.Millisecond
	maintenanceLoopTickerInterval = 1333 * time.Millisecond

	// scheduleSpawnLimit caps how many schedules of one kind a single
	// maintenance tick may fire.
	scheduleSpawnLimit uint16 = 100
)

// Task parameters
type Task struct {
	ID           int64
	Kind         int16
	Payload      []byte
	attemptsLeft uint16
	ExternalKey  string
	RepeatPeriod uint32
}

// IsLastAttempt returns true, if it's a last try
func (t *Task) IsLastAttempt() bool {
	return t.attemptsLeft == 0
}

// TaskHandler describes task handler interface.
type TaskHandler interface {
	HandleTask(ctx context.Context, task *Task) error
}

// Options task options
type Options struct {
	// name
	Name string
	// workers, 1 by default.
	WorkerCount uint16
	// The global limit on the number of concurrently executing tasks.
	// By default 0 means no limit.
	ConcurrencyLimit uint16
	// Amount of retries, default 1.
	// If all retries are passed, status.ClosedNoAttemptsLeft error returns.
	MaxAttempts uint16
	// Timeout for attempting to complete the task. Restricts the task execution
	// context passed to the handler. A task that runs longer than
	// AttemptLimitSeconds seconds is considered lost and returned to the queue
	// in the status.OpenMustRetry, after which it can be started by another
	// worker on any machine. If this was the last attempt, then the task ends
	// with status.ClosedLost.
	AttemptLimitSeconds uint32
	// Delay in seconds before the next attempt to run the task after an error.
	DelayAfterRefusedSeconds uint32
	// Sequence of delays in seconds before the next attempt to execute the
	// task after an error. After the first unsuccessful attempt, the value
	// of the first element of the array will be used, after the second, the
	// value of the second, and so on. Takes precedence over DelayAfterRefusedSeconds.
	ProgressiveDelayAfterRefusedSeconds []uint32
	// TTL in seconds from the creation of the task, after which the unfinished
	// task goes into status.ClosedExpired. By default, it is equal to the number
	// of seconds that will be spent on all MaxAttempts attempts, taking into
	// account the AttemptLimitSeconds and DelayAfterRefusedSeconds,
	// ProgressiveDelayAfterRefusedSeconds parameters.
	TTLSeconds uint32
}

// ScheduleOptions overrides kind options for the tasks that a schedule spawns.
type ScheduleOptions struct {
	// Amount of retries for a spawned task. 0 means inherit from the kind.
	MaxAttempts uint16
	// TTL in seconds for a spawned task. 0 means inherit from the kind.
	TTLSeconds uint32
}

type kindDescription struct {
	handler TaskHandler
	opts    Options
}

type storage interface {
	createTask(ctx context.Context, kind int16, maxAttempts uint16, payload []byte, ttlSeconds uint32, key string, delay time.Duration, repeatPeriod uint32) error
	createTaskTx(ctx context.Context, tx sqlx.Tx, kind int16, maxAttempts uint16, payload []byte, ttlSeconds uint32, key string, delay time.Duration, repeatPeriod uint32) error
	getTasks(ctx context.Context, kind int16, workerCountLimitForInstance uint16, workerCountLimitForQueueKind uint16) ([]*Task, error)
	completeTask(ctx context.Context, id int64) error
	refuseTask(ctx context.Context, id int64, reason string, delaySeconds uint32) error
	abortTask(ctx context.Context, id int64, reason string) error
	cancelTaskByKey(ctx context.Context, kind int16, key string, reason string) error
	closeExpiredTasks(ctx context.Context, kind int16) error
	repairLostTasks(ctx context.Context, kind int16, lossSeconds uint32) error
	archiveClosedTasks(ctx context.Context, kind int16, waitingHours uint16) error
	addRetriesToFailedTasks(ctx context.Context, kind int16, retryCnt int64) error
	upsertSchedule(ctx context.Context, kind int16, name string, cronExpr string, payload []byte, maxAttempts uint16, ttlSeconds uint32, nextFireAt time.Time) error
	deleteSchedule(ctx context.Context, kind int16, name string) error
	spawnDueScheduledTasks(ctx context.Context, kind int16, maxAttempts uint16, ttlSeconds uint32, limit uint16) (int, int, error)
}

type workerMonitor interface {
	getRestingCount() uint16
	decreaseRestingCount()
	increaseRestingCount()
}

// processor processes tasks from the queue.
type processor struct {
	kindData         map[int16]kindDescription
	storage          storage
	metricsCollector MetricsCollector
}

// RegisterKind registers the kind of tasks in the processor. For each type of task,
// an independent processing cycle is launched with its own pool of workers.
func (qp *processor) RegisterKind(kind int16, handler TaskHandler, opts Options) *processor {
	if qp == nil {
		return nil
	}

	setDefaultsForOptions(&opts)

	qp.kindData[kind] = kindDescription{
		handler: handler,
		opts:    opts,
	}
	return qp
}

func setDefaultsForOptions(opts *Options) {
	if opts.WorkerCount == 0 {
		opts.WorkerCount = defaultWorkerCount
	}

	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = defaultMaxAttempts
	}

	if opts.TTLSeconds == 0 {
		ttl := opts.AttemptLimitSeconds + uint32(opts.MaxAttempts-1)*(opts.DelayAfterRefusedSeconds+opts.AttemptLimitSeconds)

		if len(opts.ProgressiveDelayAfterRefusedSeconds) != 0 {
			ttl = opts.AttemptLimitSeconds
			for attemptIndex := uint16(1); attemptIndex < opts.MaxAttempts; attemptIndex++ {
				delayIndex := attemptIndex - 1
				delay := getDelayByAttemptIndex(opts.ProgressiveDelayAfterRefusedSeconds, delayIndex)
				ttl += delay + opts.AttemptLimitSeconds
			}
		}

		opts.TTLSeconds = ttl
	}
}

func (qp *processor) SetJSONLogFormat() {
	logger.SetJSONFormatter()
}

func (qp *processor) SetLogLevel(level LogLevel) {
	logger.SetLevel(logrus.Level(level))
}

// Start starts the task loop and returns a channel that closes when the loop stops.
// The loop continues until the context is closed.
func (qp *processor) Start(ctx context.Context) (<-chan struct{}, error) {
	done := make(chan struct{})
	go func() {
		qp.runLoop(ctx)
		close(done)
	}()
	return done, nil
}

// AppendTask publishes a task to the queue.
func (qp *processor) AppendTask(ctx context.Context, kind int16, payload []byte) error {
	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	err := qp.storage.createTask(ctx, kind, kindData.opts.MaxAttempts, payload, kindData.opts.TTLSeconds, emptyKey, zeroDuration, zeroRepeatPeriod)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}

// AppendTaskTx publishes a task to the queue in transaction.
func (qp *processor) AppendTaskTx(ctx context.Context, tx sqlx.Tx, kind int16, payload []byte) error {
	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	err := qp.storage.createTaskTx(ctx, tx, kind, kindData.opts.MaxAttempts, payload, kindData.opts.TTLSeconds, emptyKey, zeroDuration, zeroRepeatPeriod)
	if err != nil {
		return errors.Wrap(err, "storage.createTaskTx error")
	}

	return nil
}

// CancelTaskByKey sets all pending tasks that have a key to status.Closed Cancelled.
func (qp *processor) CancelTaskByKey(ctx context.Context, kind int16, key string) error {
	err := qp.storage.cancelTaskByKey(ctx, kind, key, "cancel by key")
	if err != nil {
		return errors.Wrap(err, "storage.cancelTaskByKey error")
	}

	return nil
}

// AppendTaskWithOptions publishes a task to the queue. Using the opts parameter, you can override the
// maximum number of attempts and TTL for this task, using appendOpts you can pass the task
// publication parameters in the opts parameter,
func (qp *processor) AppendTaskWithOptions(ctx context.Context, kind int16, payload []byte, opts *Options, appendOpts *AppendTaskOptions) error {
	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	key := emptyKey
	delay := zeroDuration
	repeatPeriod := zeroRepeatPeriod
	if appendOpts != nil {
		if appendOpts.ExternalKey != "" {
			key = appendOpts.ExternalKey
		}
		if appendOpts.Delay != 0 {
			delay = appendOpts.Delay
		}
		if appendOpts.RepeatPeriod != 0 {
			repeatPeriod = appendOpts.RepeatPeriod
		}
	}

	useOpts := kindData.opts
	if opts != nil {
		useOpts = *opts
	}

	err := qp.storage.createTask(ctx, kind, useOpts.MaxAttempts, payload, useOpts.TTLSeconds, key, delay, repeatPeriod)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}

// AppendTaskWithOptionsTx works as AppendTaskWithOptions in transaction
func (qp *processor) AppendTaskWithOptionsTx(ctx context.Context, tx sqlx.Tx, kind int16, payload []byte, opts *Options,
	appendOpts *AppendTaskOptions) error {
	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	key := emptyKey
	delay := zeroDuration
	repeatPeriod := zeroRepeatPeriod
	if appendOpts != nil {
		if appendOpts.ExternalKey != "" {
			key = appendOpts.ExternalKey
		}
		if appendOpts.Delay != 0 {
			delay = appendOpts.Delay
		}
		if appendOpts.RepeatPeriod != 0 {
			repeatPeriod = appendOpts.RepeatPeriod
		}
	}

	useOpts := kindData.opts
	if opts != nil {
		useOpts = *opts
	}

	err := qp.storage.createTaskTx(ctx, tx, kind, useOpts.MaxAttempts, payload, useOpts.TTLSeconds, key, delay, repeatPeriod)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}

// ScheduleTask registers a recurring task described by a cron expression, or
// updates the schedule that already has this name. The task is published by the
// processor itself whenever the schedule comes due.
//
// cronExpr accepts the standard five-field spec ("0 0 * * *" runs the task every
// day at 00:00), the "@daily"/"@hourly" descriptors, "@every 30m" intervals and
// an optional "CRON_TZ=Europe/Moscow" prefix.
//
// The call is idempotent on (kind, name), so it is meant to be made on every
// service start. Three properties are worth knowing:
//
//   - a schedule never fires at the moment it is created: the first task is
//     published at the next moment the expression matches;
//   - a fire missed while no processor was running is caught up once, not once
//     per moment that passed;
//   - a fire that comes due while the previous task of the same schedule is
//     still open is skipped.
func (qp *processor) ScheduleTask(ctx context.Context, kind int16, name string, cronExpr string, payload []byte, opts *ScheduleOptions) error {
	if _, ok := qp.kindData[kind]; !ok {
		return ErrUnexpectedTaskKind
	}

	if name == "" {
		return errors.New("schedule name must not be empty")
	}

	nextFireAt, err := nextCronFire(cronExpr, time.Now())
	if err != nil {
		return err
	}

	var useOpts ScheduleOptions
	if opts != nil {
		useOpts = *opts
	}

	err = qp.storage.upsertSchedule(ctx, kind, name, cronExpr, payload, useOpts.MaxAttempts, useOpts.TTLSeconds, nextFireAt)
	if err != nil {
		return errors.Wrap(err, "storage.upsertSchedule error")
	}

	return nil
}

// UnscheduleTask removes the schedule with the given name. A task that this
// schedule has already published is left to finish.
func (qp *processor) UnscheduleTask(ctx context.Context, kind int16, name string) error {
	err := qp.storage.deleteSchedule(ctx, kind, name)
	if err != nil {
		return errors.Wrap(err, "storage.deleteSchedule error")
	}

	return nil
}

// AddRetries returns tasks with the specified kind and status.ClosedExpired,
// status.ClosedNoAttemptsLeft to the queue, setting a new number of retriesCnt attempts for them.
func (qp *processor) AddRetries(ctx context.Context, kind int16, retriesCnt int64) error {
	err := qp.storage.addRetriesToFailedTasks(ctx, kind, retriesCnt)
	return errors.Wrap(err, "cannot add retries")
}

func (qp *processor) runMaintenanceLoop(ctx context.Context, kind int16, opts Options) {
	attemptLimit := time.Duration(opts.AttemptLimitSeconds) * time.Second

	logger.Warnf(ctx, "%s: enter maintenance loop (kind=%d attempt_limit=%v)", opts.Name, kind, attemptLimit)
	defer logger.Warnf(ctx, "%s: escape maintenance loop", opts.Name)

	ticker := time.NewTicker(maintenanceLoopTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			processCtx := logger.WrapContext(ctx, opts.Name)
			qp.processMaintenance(processCtx, kind, opts)
		case <-ctx.Done():
			return
		}
	}
}

func (qp *processor) runWorkerPool(ctx context.Context, monitor workerMonitor, tasks <-chan *Task, handler TaskHandler, opts Options) {
	for i := uint16(0); i < opts.WorkerCount; i++ {
		worker := newWorker(
			i,
			opts.Name,
			opts.Name,
			qp.storage,
			monitor,
			tasks,
			opts.MaxAttempts,
			opts.AttemptLimitSeconds,
			opts.DelayAfterRefusedSeconds,
			opts.ProgressiveDelayAfterRefusedSeconds,
			qp.metricsCollector,
		)
		go worker.run(ctx, handler)
	}
}

func (qp *processor) runTaskFetcher(ctx context.Context, monitor workerMonitor, tasks chan<- *Task, kind int16, opts Options) {
	attemptLimit := time.Duration(opts.AttemptLimitSeconds) * time.Second

	logger.Debugf(ctx, "%s: enter task processing loop (kind=%d attempt_limit=%v worker_count=%v)", opts.Name, kind, attemptLimit, opts.WorkerCount)
	defer logger.Debugf(ctx, "%s: escape task processing loop", opts.Name)

	ticker := time.NewTicker(taskFetcherTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			restingWorkerCount := monitor.getRestingCount()
			if restingWorkerCount == 0 {
				logger.Debugf(ctx, "%s: runTaskFetcher: no resting workers", opts.Name)
				break // select
			}

			fetchedTasks, err := qp.storage.getTasks(ctx, kind, restingWorkerCount, opts.ConcurrencyLimit)
			if err != nil {
				logger.Errorf(ctx, "%s: runTaskFetcher: storage.getTasks error: %v", opts.Name, err)
				break // select
			}

			for _, task := range fetchedTasks {
				tasks <- task
			}
		case <-ctx.Done():
			return
		}
	}
}

func (qp *processor) runLoop(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(len(qp.kindData))

	for kind, data := range qp.kindData {
		go qp.runMaintenanceLoop(ctx, kind, data.opts)

		monitor := newWorkerMonitor(data.opts.WorkerCount)
		tasks := make(chan *Task, data.opts.WorkerCount)

		go qp.runWorkerPool(ctx, monitor, tasks, data.handler, data.opts)

		go func(monitor workerMonitor, tasks chan<- *Task, kind int16, opts Options) {
			qp.runTaskFetcher(ctx, monitor, tasks, kind, opts)
			wg.Done()
			close(tasks)
		}(monitor, tasks, kind, data.opts)
	}

	wg.Wait()
}

// 1. publish tasks for the schedules that came due
// 2. closed expired tasks (tasks woth expire date earlier than now)
// 3. repair lost tasks (if task performs more than AttemptLimitSeconds then sets OpenMustRetry or ClosedLost status)
// 4. delete all successful tasks that was finished more than week ago
func (qp *processor) processMaintenance(ctx context.Context, kind int16, opts Options) {
	spawned, skipped, err := qp.storage.spawnDueScheduledTasks(ctx, kind, opts.MaxAttempts, opts.TTLSeconds, scheduleSpawnLimit)
	if err != nil {
		logger.Errorf(ctx, "storage.spawnDueScheduledTasks error: %v", err)
	} else if spawned != 0 || skipped != 0 {
		logger.Infof(ctx, "spawned %d scheduled tasks, skipped %d fires (kind=%d)", spawned, skipped, kind)
	}

	err = qp.storage.closeExpiredTasks(ctx, kind)
	if err != nil {
		logger.Errorf(ctx, "storage.closeExpiredTasks error: %v", err)
	}

	err = qp.storage.repairLostTasks(ctx, kind, opts.AttemptLimitSeconds)
	if err != nil {
		logger.Errorf(ctx, "storage.repairLostTasks error: %v", err)
	}

	err = qp.storage.archiveClosedTasks(ctx, kind, 7*24)
	if err != nil {
		logger.Errorf(ctx, "storage.archiveClosedTasks error: %v", err)
	}
}
