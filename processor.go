package pgqueue

import (
	"context"
	"sync"
	"time"

	"github.com/kasorse/pgqueue/internal/logger"
	"github.com/pkg/errors"
)

const (
	taskFetcherTickerInterval     = 1111 * time.Millisecond
	maintenanceLoopTickerInterval = 1333 * time.Millisecond
)

// Task parameters
type Task struct {
	ID           int64
	Kind         int16
	Payload      []byte
	attemptsLeft uint16
	ExternalKey  string
}

// IsLastAttempt returns true, if it's a last try —
// последняя.
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
	// Таймаут попытки выполнения задачи. Ограничивает контекст выполнения
	// задачи, передаваемый в обработчик. Задача, которая выполняется дольше
	// AttemptLimitSeconds секунд, считается потерянной и возвращается в
	// очередь в статусе status.OpenMustRetry, после чего ее может начать
	// выполнять другой воркер на любой машине. Если эта попытка была
	// последней, то задача завершается со статусом status.ClosedLost.
	AttemptLimitSeconds uint32
	// Задержка в секундах перед следующей попыткой выполнения задачи после
	// ошибки.
	DelayAfterRefusedSeconds uint32
	// Последовательность задержек в секундах перед следующей попыткой
	// выполнения задачи после ошибки. После первой неудачной попытки будет
	// использовано значение первого элемента массива, после второй — значение
	// второго и т.д. Имеет приоритет над DelayAfterRefusedSeconds.
	ProgressiveDelayAfterRefusedSeconds []uint32
	// TTL в секундах от создания задачи, после которого незавершенная
	// задача переходит в статус status.ClosedExpired. По умолчанию равен
	// числу секунд, которое будет затрачено на все MaxAttempts попыток с
	// учетом параметров AttemptLimitSeconds и DelayAfterRefusedSeconds,
	// ProgressiveDelayAfterRefusedSeconds.
	TTLSeconds uint32
}

type kindDescription struct {
	handler TaskHandler
	opts    Options
}

type storage interface {
	createTask(ctx context.Context, kind int16, maxAttempts uint16, payload []byte, ttlSeconds uint32, key string, delay time.Duration) error
	getTasks(ctx context.Context, kind int16, workerCountLimitForInstance uint16, workerCountLimitForQueueKind uint16) ([]*Task, error)
	completeTask(ctx context.Context, id int64) error
	refuseTask(ctx context.Context, id int64, reason string, delaySeconds uint32) error
	abortTask(ctx context.Context, id int64, reason string) error
	cancelTaskByKey(ctx context.Context, kind int16, key string, reason string) error
	closeExpiredTasks(ctx context.Context, kind int16) error
	repairLostTasks(ctx context.Context, kind int16, lossSeconds uint32) error
	archiveClosedTasks(ctx context.Context, kind int16, waitingHours uint16) error
	addRetriesToFailedTasks(ctx context.Context, kind int16, retryCnt int64) error
}

type workerMonitor interface {
	getRestingCount() uint16
	decreaseRestingCount()
	increaseRestingCount()
}

// processor обрабатывает задачи из очереди.
type processor struct {
	kindData         map[int16]kindDescription
	storage          storage
	metricsCollector MetricsCollector
}

// RegisterKind регистрирует вид задач в процессоре. Для каждого вида задач
// запускается независимый цикл обработки со своим пулом воркеров.
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

// Start запускает цикл обработки задач и возвращает канал, который закрывается
// после остановки цикла. Цикл продолжается до тех пор, пока не закрыт контекст
// ctx.
func (qp *processor) Start(ctx context.Context) (<-chan struct{}, error) {
	done := make(chan struct{})
	go func() {
		qp.runLoop(ctx)
		close(done)
	}()
	return done, nil
}

// AppendTask публикует задачу в очередь. Опционально можно передать параметры
// публикации задачи в параметре opts, в этом случае будет использовано только
// первое значение параметра.
func (qp *processor) AppendTask(ctx context.Context, kind int16, payload []byte, opts ...AppendTaskOptions) error {
	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	key := emptyKey
	delay := zeroDuration
	if len(opts) > 0 {
		opt := opts[0]
		if opt.ExternalKey != "" {
			key = opt.ExternalKey
		}
		if opt.Delay != 0 {
			delay = opt.Delay
		}
	}

	err := qp.storage.createTask(ctx, kind, kindData.opts.MaxAttempts, payload, kindData.opts.TTLSeconds, key, delay)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}

// CancelTaskByKey переводит все незавершенные задачи, имеющие ключ key, в
// статус status.ClosedCancelled.
func (qp *processor) CancelTaskByKey(ctx context.Context, kind int16, key string) error {
	err := qp.storage.cancelTaskByKey(ctx, kind, key, "cancel by key")
	if err != nil {
		return errors.Wrap(err, "storage.cancelTaskByKey error")
	}

	return nil
}

// AppendTaskWithOptions публикует задачу в очередь. C помощью параметра opts
// можно переопределить для этой задачи максимальное количество попыток и TTL,
// в этом случае будет использовано только первое значение параметра.
func (qp *processor) AppendTaskWithOptions(ctx context.Context, kind int16, payload []byte, opts ...Options) error {
	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	useOpts := kindData.opts
	if len(opts) > 0 {
		useOpts = opts[0]
	}

	err := qp.storage.createTask(ctx, kind, useOpts.MaxAttempts, payload, useOpts.TTLSeconds, emptyKey, zeroDuration)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}

// AddRetries возвращает задачи с указанным kind и в статусах
// status.ClosedExpired, status.ClosedNoAttemptsLeft в очередь, устанавливая
// для них новое число попыток retriesCnt.
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

	logger.Warnf(ctx, "%s: enter task processing loop (kind=%d attempt_limit=%v worker_count=%v)", opts.Name, kind, attemptLimit, opts.WorkerCount)
	defer logger.Warnf(ctx, "%s: escape task processing loop", opts.Name)

	ticker := time.NewTicker(taskFetcherTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			restingWorkerCount := monitor.getRestingCount()
			if restingWorkerCount == 0 {
				logger.Infof(ctx, "%s: runTaskFetcher: no resting workers", opts.Name)
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
		}(monitor, tasks, kind, data.opts)
	}

	wg.Wait()
}

func (qp *processor) processMaintenance(ctx context.Context, kind int16, opts Options) {
	err := qp.storage.closeExpiredTasks(ctx, kind)
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
