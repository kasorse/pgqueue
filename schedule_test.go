package pgqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kasorse/pgqueue/status"
	"github.com/stretchr/testify/suite"
)

const (
	scheduleNameForTest = "nightly"
	scheduleKindForTest = int16(7)
)

type ScheduleStorageSuite struct {
	suite.Suite

	ctx     context.Context
	sqlDB   *sqlx.DB
	storage *sqlStorage
}

func TestScheduleStorage(t *testing.T) {
	suite.Run(t, new(ScheduleStorageSuite))
}

func (s *ScheduleStorageSuite) SetupSuite() {
	s.ctx = context.Background()
	s.sqlDB = setUp(s.T())
	s.storage = newSQLStorage(s.sqlDB)
}

func (s *ScheduleStorageSuite) TearDownSuite() {
	tearDown(s.sqlDB)
}

func (s *ScheduleStorageSuite) SetupTest() {
	clean(s.T(), s.sqlDB)
}

// schedule reads back the single stored schedule.
func (s *ScheduleStorageSuite) schedule() (id int64, cronExpr string, nextFireAt time.Time, enabled bool) {
	err := s.sqlDB.QueryRowContext(s.ctx, `
		SELECT id, cron, next_fire_at, enabled FROM queue_schedule WHERE kind = $1 AND name = $2
	`, scheduleKindForTest, scheduleNameForTest).Scan(&id, &cronExpr, &nextFireAt, &enabled)
	s.Require().NoError(err)
	return id, cronExpr, nextFireAt, enabled
}

func (s *ScheduleStorageSuite) openTaskCount(scheduleID int64) int {
	var count int
	err := s.sqlDB.QueryRowContext(s.ctx, `
		SELECT COUNT(*) FROM queue WHERE schedule_id = $1 AND status <= $2
	`, scheduleID, status.OpenClosedStatusesDivide).Scan(&count)
	s.Require().NoError(err)
	return count
}

func (s *ScheduleStorageSuite) upsert(cronExpr string) {
	nextFireAt, err := nextCronFire(cronExpr, time.Now())
	s.Require().NoError(err)
	s.Require().NoError(s.storage.upsertSchedule(s.ctx, scheduleKindForTest, scheduleNameForTest, cronExpr,
		[]byte(`{"a":1}`), 0, 0, nextFireAt))
}

// A repeated call with the same expression must not move the pending fire, so
// that restarting a service does not shift its schedules.
func (s *ScheduleStorageSuite) TestUpsertIsIdempotent() {
	s.upsert("@every 1h")
	firstID, _, firstFireAt, _ := s.schedule()

	time.Sleep(1100 * time.Millisecond)
	s.upsert("@every 1h")

	secondID, cronExpr, secondFireAt, enabled := s.schedule()
	s.Equal(firstID, secondID, "upsert must update the existing row")
	s.Equal("@every 1h", cronExpr)
	s.True(enabled)
	s.WithinDuration(firstFireAt, secondFireAt, time.Millisecond, "pending fire must not move")
}

func (s *ScheduleStorageSuite) TestUpsertMovesFireWhenExpressionChanges() {
	s.upsert("@every 3h")
	_, _, firstFireAt, _ := s.schedule()

	s.upsert("@every 1h")

	_, cronExpr, secondFireAt, _ := s.schedule()
	s.Equal("@every 1h", cronExpr)
	s.True(secondFireAt.Before(firstFireAt), "a changed expression must re-derive the next fire")
}

func (s *ScheduleStorageSuite) TestSpawnSkipsWhilePreviousTaskIsOpen() {
	// @every 1s so that the schedule is due again immediately after each fire
	s.upsert("@every 1s")
	scheduleID, _, _, _ := s.schedule()

	// not due yet
	spawned, skipped, err := s.storage.spawnDueScheduledTasks(s.ctx, scheduleKindForTest, maxAttempts, 60, scheduleSpawnLimit)
	s.Require().NoError(err)
	s.Equal(0, spawned)
	s.Equal(0, skipped)

	time.Sleep(1100 * time.Millisecond)

	spawned, skipped, err = s.storage.spawnDueScheduledTasks(s.ctx, scheduleKindForTest, maxAttempts, 60, scheduleSpawnLimit)
	s.Require().NoError(err)
	s.Equal(1, spawned)
	s.Equal(0, skipped)
	s.Equal(1, s.openTaskCount(scheduleID))

	// the previous task is still open, so this fire is dropped instead of queued
	time.Sleep(1100 * time.Millisecond)

	spawned, skipped, err = s.storage.spawnDueScheduledTasks(s.ctx, scheduleKindForTest, maxAttempts, 60, scheduleSpawnLimit)
	s.Require().NoError(err)
	s.Equal(0, spawned)
	s.Equal(1, skipped)
	s.Equal(1, s.openTaskCount(scheduleID), "at most one open task per schedule")

	// once it closes, the next fire spawns again
	tasks, err := s.storage.getTasks(s.ctx, scheduleKindForTest, 1, 0)
	s.Require().NoError(err)
	s.Require().Len(tasks, 1)
	s.Require().NoError(s.storage.completeTask(s.ctx, tasks[0].ID))
	s.Equal(0, s.openTaskCount(scheduleID))

	time.Sleep(1100 * time.Millisecond)

	spawned, skipped, err = s.storage.spawnDueScheduledTasks(s.ctx, scheduleKindForTest, maxAttempts, 60, scheduleSpawnLimit)
	s.Require().NoError(err)
	s.Equal(1, spawned)
	s.Equal(0, skipped)
}

func (s *ScheduleStorageSuite) TestSpawnDisablesUnusableSchedule() {
	s.upsert("@every 1s")

	// simulate a row edited outside ScheduleTask
	_, err := s.sqlDB.ExecContext(s.ctx, `
		UPDATE queue_schedule SET cron = 'not a cron expression' WHERE kind = $1 AND name = $2
	`, scheduleKindForTest, scheduleNameForTest)
	s.Require().NoError(err)

	time.Sleep(1100 * time.Millisecond)

	spawned, skipped, err := s.storage.spawnDueScheduledTasks(s.ctx, scheduleKindForTest, maxAttempts, 60, scheduleSpawnLimit)
	s.Require().NoError(err)
	s.Equal(0, spawned)
	s.Equal(0, skipped)

	_, _, _, enabled := s.schedule()
	s.False(enabled, "a schedule that cannot report its next fire must be disabled, not retried forever")
}

func (s *ScheduleStorageSuite) TestDeleteSchedule() {
	s.upsert("@every 1h")
	s.Require().NoError(s.storage.deleteSchedule(s.ctx, scheduleKindForTest, scheduleNameForTest))

	var count int
	err := s.sqlDB.QueryRowContext(s.ctx, `SELECT COUNT(*) FROM queue_schedule`).Scan(&count)
	s.Require().NoError(err)
	s.Equal(0, count)
}

// TestScheduleTask_e2e drives a schedule through the running processor: the
// handler must be called repeatedly without the caller ever publishing a task,
// and must stop once the schedule is removed.
func TestScheduleTask_e2e(t *testing.T) {
	ctx := context.Background()

	sqlDB := setUp(t)
	defer tearDown(sqlDB)
	clean(t, sqlDB)

	var (
		mu    sync.Mutex
		calls int
	)
	handler := taskHandlerFunc(func(_ context.Context, _ *Task) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil
	})
	callCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}

	qp := MakeSQLProcessor(sqlDB)
	qp.RegisterKind(scheduleKindForTest, handler, Options{
		Name:                "schedule processor",
		AttemptLimitSeconds: 3,
		MaxAttempts:         maxAttempts,
	})

	qpContext, stopQP := context.WithCancel(ctx)
	defer stopQP()

	qpDone, startErr := qp.Start(qpContext)
	if startErr != nil {
		t.Fatalf("cannot start processor: %v", startErr)
	}

	if err := qp.ScheduleTask(ctx, scheduleKindForTest, scheduleNameForTest, "@every 2s", []byte(`{}`), nil); err != nil {
		t.Fatalf("cannot schedule task: %v", err)
	}

	// @every 2s over 9s, minus the fire that never happens at creation time
	time.Sleep(9 * time.Second)

	afterSchedule := callCount()
	if afterSchedule < 2 {
		t.Fatalf("expected the schedule to fire more than once, got %d calls", afterSchedule)
	}

	var badStatusCount int
	err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM queue WHERE schedule_id IS NOT NULL AND status <> $1
	`, status.ClosedSuccess).Scan(&badStatusCount)
	if err != nil {
		t.Fatalf("cannot count spawned tasks: %v", err)
	}
	if badStatusCount != 0 {
		t.Errorf("every spawned task must end in success, %d did not", badStatusCount)
	}

	if err := qp.UnscheduleTask(ctx, scheduleKindForTest, scheduleNameForTest); err != nil {
		t.Fatalf("cannot unschedule task: %v", err)
	}

	unscheduledAt := callCount()
	time.Sleep(5 * time.Second)
	if got := callCount(); got != unscheduledAt {
		t.Errorf("expected no calls after UnscheduleTask, got %d more", got-unscheduledAt)
	}

	stopQP()
	<-qpDone

	clean(t, sqlDB)
}

type taskHandlerFunc func(ctx context.Context, task *Task) error

func (f taskHandlerFunc) HandleTask(ctx context.Context, task *Task) error {
	return f(ctx, task)
}

func TestScheduleTaskValidation(t *testing.T) {
	ctx := context.Background()

	qp := &processor{kindData: make(map[int16]kindDescription)}
	qp.kindData[scheduleKindForTest] = kindDescription{}

	if err := qp.ScheduleTask(ctx, scheduleKindForTest+1, scheduleNameForTest, "@every 1h", nil, nil); err != ErrUnexpectedTaskKind {
		t.Errorf("expected ErrUnexpectedTaskKind for an unregistered kind, got %v", err)
	}

	if err := qp.ScheduleTask(ctx, scheduleKindForTest, "", "@every 1h", nil, nil); err == nil {
		t.Error("expected an error for an empty schedule name")
	}

	if err := qp.ScheduleTask(ctx, scheduleKindForTest, scheduleNameForTest, "not a cron expression", nil, nil); err == nil {
		t.Error("expected an error for an invalid cron expression")
	}
}
