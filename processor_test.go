package pgqueue

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestProcessor_e2e(t *testing.T) {
	a := assert.New(t)

	var someTaskKind int16 = 1

	testCases := []struct {
		tasks   []TaskID
		handler *testHandler // implements TaskHandler, struct is used for assertions
		timeout time.Duration
	}{
		{
			tasks: []TaskID{
				111,
				222,
				333,
			},
			handler: newHandler(t, map[TaskID][]error{
				// success on first try
				111: {
					nil,
				},

				// all errors, cannot finish task
				222: makeErrorSlice(errForTest, maxAttempts),

				// only one call is made
				333: {
					errors.Wrap(ErrMustAbortTask, "fff"),
				},
			}),
			timeout: time.Second * 10,
		},
	}

	// This test creates a bunch of tasks and controls order of execution using mock handlers that report progress
	sqlDB := setUp(t)

	for _, tc := range testCases {
		ctx := context.Background()

		qp := MakeSQLProcessor(sqlDB)

		qp.RegisterKind(someTaskKind, tc.handler, Options{
			Name:                     "application processor",
			AttemptLimitSeconds:      3, // all tasks will finish almost instantly
			MaxAttempts:              maxAttempts,
			DelayAfterRefusedSeconds: 1,
		})

		qpContext, stopQP := context.WithCancel(ctx)

		qpDone, err := qp.Start(qpContext)
		a.NoError(err)

		for _, task := range tc.tasks {
			a.NoError(qp.AppendTask(ctx, someTaskKind, task.toPayload()), "cannot add task")
		}
		time.Sleep(tc.timeout)
		// assert that number of calls is correct
		tc.handler.checkAttemptsCount()

		stopQP() // посылаем сигнал для завершения queue processor
		<-qpDone // ждем завершения queue processor

		clean(t, sqlDB)
	}

	tearDown(sqlDB)
}

func makeErrorSlice(err error, length int) []error {
	ls := make([]error, 0, length)
	for i := 0; i != length; i++ {
		ls = append(ls, errors.Wrapf(err, "call %d of %d", i+1, length))
	}
	return ls
}

type TestAppendTaskWithKeySuite struct {
	suite.Suite

	ctx context.Context

	sqlDB            *sqlx.DB
	qp               *SQLProcessor
	metricsCollector MetricsCollector

	taskKind int16
	handler  *TaskHandlerMock
}

func TestCreateTaskWithKey(t *testing.T) {
	suite.Run(t, new(TestAppendTaskWithKeySuite))
}

func (s *TestAppendTaskWithKeySuite) SetupSuite() {
	s.sqlDB = setUp(s.T())
	s.ctx = context.Background()
	s.metricsCollector = NewMetricsCollectorMock(s.T()).
		IncBusyWorkersMock.Set(func(_ string, _ int16) {}).
		DecBusyWorkersMock.Set(func(_ string, _ int16) {}).
		MeasureTaskDurationMock.Set(func(
		_ string,
		_ int16,
		_ bool,
		_ time.Duration,
	) {
	},
	)

	s.taskKind = 1
	s.qp = MakeSQLProcessor(s.sqlDB)
	s.qp.SetMetricsCollector(s.metricsCollector)

	s.handler = NewTaskHandlerMock(s.T())

	s.qp.RegisterKind(
		s.taskKind,
		s.handler,
		Options{
			Name:                "test",
			AttemptLimitSeconds: 10, // all tasks will finish almost instantly
			MaxAttempts:         1,
		},
	)

	_, err := s.qp.Start(s.ctx)
	s.NoError(err)
}

func (s *TestAppendTaskWithKeySuite) TearDownSuite() {
	tearDown(s.sqlDB)
}

func (s *TestAppendTaskWithKeySuite) TestExternalKeySet() {
	// arrange
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().Add(time.Second*5))

	expectedExternalKey := "mega_key"
	var actualExternalKey string

	s.handler.HandleTaskMock.Set(func(ctx context.Context, task *Task) (err error) {
		actualExternalKey = task.ExternalKey
		cancel()
		return nil
	})

	// act
	err := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{ExternalKey: expectedExternalKey})

	<-ctx.Done()

	// assert
	s.NoError(err)
	s.Equal(expectedExternalKey, actualExternalKey)
}

func (s *TestAppendTaskWithKeySuite) TestDelay() {
	// arrange
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().Add(time.Second*7))

	var actualDelay time.Duration
	expectedDelay := time.Second * 5

	start := time.Now()
	s.handler.HandleTaskMock.Set(func(ctx context.Context, task *Task) (err error) {
		actualDelay = time.Since(start)
		cancel()
		return nil
	})

	// act
	err := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{
		ExternalKey: "mega_key",
		Delay:       expectedDelay,
	})

	<-ctx.Done()

	// assert
	s.NoError(err)
	s.InDelta(expectedDelay.Seconds(), actualDelay.Seconds(), 1.5)
}

func (s *TestAppendTaskWithKeySuite) TestCancel() {
	// arrange
	ctx, cancelCtx := context.WithDeadline(s.ctx, time.Now().Add(time.Second*4))
	defer cancelCtx()

	keyToCancel := "cancel_key"
	keyToContinue := "continue_key"

	s.handler.HandleTaskMock.Set(func(ctx context.Context, task *Task) (err error) {
		s.Equal(keyToContinue, task.ExternalKey)
		return nil
	})

	err1 := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{ExternalKey: keyToCancel, Delay: time.Second * 2})
	err2 := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{ExternalKey: keyToCancel, Delay: time.Second * 2})
	err3 := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{ExternalKey: keyToContinue, Delay: time.Second * 2})
	err4 := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{ExternalKey: keyToContinue, Delay: time.Second * 2})
	s.NoError(err1)
	s.NoError(err2)
	s.NoError(err3)
	s.NoError(err4)

	expectedCallsAmount := 2

	// act
	err := s.qp.CancelTaskByKey(ctx, s.taskKind, keyToCancel)

	<-ctx.Done()

	// assert
	s.NoError(err)
	s.Equal(expectedCallsAmount, len(s.handler.HandleTaskMock.Calls()))
}

func (s *TestAppendTaskWithKeySuite) TestEmptyExternalKey() {
	// arrange
	ctx, cancelCtx := context.WithDeadline(s.ctx, time.Now().Add(time.Second*4))
	defer cancelCtx()

	emptyKey := ""

	s.handler.HandleTaskMock.Set(func(ctx context.Context, task *Task) (err error) {
		s.Equal(emptyKey, task.ExternalKey)
		return nil
	})

	// act
	err := s.qp.AppendTaskWithOptions(ctx, s.taskKind, []byte("{}"), nil, &AppendTaskOptions{ExternalKey: emptyKey})

	<-ctx.Done()

	// assert
	s.NoError(err)
}
