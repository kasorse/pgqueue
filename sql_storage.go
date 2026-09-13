package pgqueue

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kasorse/pgqueue/internal/logger"
	"github.com/kasorse/pgqueue/status"
	"github.com/pkg/errors"
)

type sqlStorage struct {
	db *sqlx.DB
}

func newSQLStorage(
	db *sqlx.DB,
) *sqlStorage {
	return &sqlStorage{
		db: db,
	}
}

type txFunc func(ctx context.Context, tx *sqlx.Tx) error

func (s *sqlStorage) withTransaction(ctx context.Context, name string, fn txFunc) (err error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			rollbackTx(ctx, tx)
			panic(p)
		} else if err != nil {
			rollbackTx(ctx, tx)
		} else {
			err = tx.Commit()
		}
	}()

	err = fn(ctx, tx)
	return err
}

func rollbackTx(ctx context.Context, tx *sqlx.Tx) {
	if err := tx.Rollback(); err != nil {
		logger.Errorf(ctx, "cannot rollback transaction: %v", err)
	}
}

func (s *sqlStorage) createTask(ctx context.Context, kind int16, maxAttempts uint16, payload []byte,
	ttlSeconds uint32, externalKey string, delay time.Duration, repeatPeriod uint32) error {

	insertQuery := `
		INSERT INTO public.queue (kind, attempts_left, payload, expires_at, external_key, delayed_till,
				repeat_period)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (kind, external_key) WHERE status <= 3 DO NOTHING
	`

	delayedTill := time.Now().Add(delay)
	expiresAt := delayedTill.Add(time.Duration(ttlSeconds) * time.Second)

	var nullableExternalKey *string
	if externalKey != "" {
		nullableExternalKey = &externalKey
	}

	var nullableRepeatPeriod sql.NullInt32
	if repeatPeriod != 0 {
		nullableRepeatPeriod.Valid = true
		nullableRepeatPeriod.Int32 = int32(repeatPeriod)
	}

	_, err := s.db.ExecContext(ctx, insertQuery, kind, maxAttempts, string(payload), expiresAt, nullableExternalKey, delayedTill, nullableRepeatPeriod)
	return err
}

func (s *sqlStorage) createTaskTx(ctx context.Context, tx sqlx.Tx, kind int16, maxAttempts uint16, payload []byte,
	ttlSeconds uint32, externalKey string, delay time.Duration, repeatPeriod uint32) error {
	insertQuery := `
	INSERT INTO public.queue (kind, attempts_left, payload, expires_at, external_key, delayed_till,
			repeat_period)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (kind, external_key) WHERE status <= 3 DO NOTHING
`

	delayedTill := time.Now().Add(delay)
	expiresAt := delayedTill.Add(time.Duration(ttlSeconds) * time.Second)

	var nullableExternalKey *string
	if externalKey != "" {
		nullableExternalKey = &externalKey
	}

	var nullableRepeatPeriod sql.NullInt32
	if repeatPeriod != 0 {
		nullableRepeatPeriod.Valid = true
		nullableRepeatPeriod.Int32 = int32(repeatPeriod)
	}

	res, err := tx.ExecContext(ctx, insertQuery,
		kind,                 // $1
		maxAttempts,          // $2
		string(payload),      // $3
		expiresAt,            // $4
		nullableExternalKey,  // $5
		delayedTill,          // $6
		nullableRepeatPeriod, // $7
	)
	if err != nil {
		return err
	}
	if c, err := res.RowsAffected(); err == nil && c == 0 {
		logger.Errorf(ctx, "no affected rows on insert", "kind", kind, "key", externalKey)
	}
	return nil
}

func (s *sqlStorage) getTasks(ctx context.Context, kind int16, workerCountLimitForInstance uint16, workerCountLimitForQueueKind uint16) ([]*Task, error) {
	if workerCountLimitForInstance == 0 {
		return nil, errors.New("wrong workerCountLimitForInstance")
	}

	semaphoreQuery := `
		SELECT COUNT(*)
		FROM public.queue
		WHERE kind = $1
		  AND status = $2
	`

	query := `
		UPDATE public.queue SET
			status = $4,
			attempts_left = attempts_left - 1,
			updated = $2
		WHERE id IN (
			SELECT id
			FROM public.queue
			WHERE kind = $1
			  AND status < 2
			  AND delayed_till <= $2
			  AND attempts_left > 0
			ORDER BY delayed_till ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, kind, attempts_left, payload, external_key, repeat_period
	`
	var dbTasks []*dbTask

	err := s.withTransaction(ctx, "get task from queue", func(cxt context.Context, tx *sqlx.Tx) error {
		limit := int64(workerCountLimitForInstance)

		if workerCountLimitForQueueKind > 0 {
			var openTaskCount int64

			err := tx.GetContext(ctx, &openTaskCount, semaphoreQuery,
				kind,                  // $1
				status.OpenProcessing, // $2
			)
			if err != nil {
				return err
			}

			limitByQueueKind := int64(workerCountLimitForQueueKind) - openTaskCount
			if limitByQueueKind < limit {
				limit = limitByQueueKind
			}
			// there may be more tasks in the "in progress" status than the limit set for this
			if limit <= 0 {
				dbTasks = nil // absence marker
				return nil
			}
		}

		err := tx.SelectContext(ctx, &dbTasks, query,
			kind,                  // $1
			time.Now(),            // $2
			limit,                 // $3
			status.OpenProcessing, // $4
		)
		if err != nil {
			if err == sql.ErrNoRows {
				dbTasks = nil // absence marker
				return nil
			}
			return err
		}

		return nil
	})

	return fromDBTasks(dbTasks), err
}

func (s *sqlStorage) completeTask(ctx context.Context, id int64) error {
	updateQuery := `
		UPDATE public.queue SET
			status = $2,
			updated = $3
		WHERE id = $1 and status = $4::smallint
	`

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		id,                    // $1
		status.ClosedSuccess,  // $2
		time.Now(),            // $3
		status.OpenProcessing, // $4
	)
	return err
}

func (s *sqlStorage) refuseTask(ctx context.Context, id int64, reason string, delaySeconds uint32) error {
	updateQuery := `
		UPDATE public.queue SET
			status = CASE WHEN attempts_left > 0 THEN $3::smallint ELSE $4::smallint END,
			delayed_till = $2,
			messages = array_append(messages, $5),
			updated = $6
		WHERE id = $1
	`

	now := time.Now()
	delayedTill := now.Add(time.Duration(delaySeconds) * time.Second)
	updatedAt := now

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		id,                          // $1
		delayedTill,                 // $2
		status.OpenMustRetry,        // $3
		status.ClosedNoAttemptsLeft, // $4
		reason,                      // $5
		updatedAt,                   // $6
	)
	return err
}

func (s *sqlStorage) cancelTaskByKey(ctx context.Context, kind int16, externalKey string, reason string) error {
	updateQuery := `
		UPDATE public.queue SET
			status = $4,
			updated = $5
		WHERE external_key IS NOT NULL AND external_key = $1 AND kind = $2 AND status < $3
	`

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		externalKey,                     // $1
		kind,                            // $2
		status.OpenClosedStatusesDivide, // $3
		status.ClosedCancelled,          // $4
		time.Now(),                      // $5
	)
	return err
}

func (s *sqlStorage) closeExpiredTasks(ctx context.Context, kind int16) error {
	updateQuery := `
		UPDATE public.queue SET
			status = $2
		WHERE kind = $1
		  AND status < 50
		  AND expires_at <= $3
	`

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		kind,                 // $1
		status.ClosedExpired, // $2
		time.Now(),           // $3
	)
	return err
}

func (s *sqlStorage) repairLostTasks(ctx context.Context, kind int16, lossSeconds uint32) error {
	updateQuery := `
		UPDATE public.queue SET
			status = CASE WHEN attempts_left > 0 THEN $4::smallint ELSE $5::smallint END
		WHERE kind = $1
		  AND status = $2
		  AND updated <= $3
	`

	updatedAt := time.Now().Add(-time.Duration(lossSeconds) * time.Second)

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		kind,                  // $1
		status.OpenProcessing, // $2
		updatedAt,             // $3
		status.OpenMustRetry,  // $4
		status.ClosedLost,     // $5
	)
	return err
}

func (s *sqlStorage) archiveClosedTasks(ctx context.Context, kind int16, waitingHours uint16) error {
	deleteQuery := `
		DELETE FROM public.queue
		WHERE kind = $1
		  AND status = $2
		  AND updated <= $3
	`

	updatedAt := time.Now().Add(-time.Duration(waitingHours) * time.Hour)

	_, err := s.db.ExecContext(
		ctx,
		deleteQuery,
		kind,                 // $1
		status.ClosedSuccess, // $2
		updatedAt,            // $3
	)
	return err
}

func (s *sqlStorage) abortTask(ctx context.Context, id int64, reason string) error {
	updateQuery := `
		UPDATE public.queue SET
			status = $2,
			attempts_left = 0,
			messages = array_append(messages, $3),
			updated = $4
		WHERE id = $1
	`

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		id,                   // $1
		status.ClosedAborted, // $2
		reason,               // $3
		time.Now(),           // $4
	)
	return err
}

func (s *sqlStorage) addRetriesToFailedTasks(ctx context.Context, kind int16, retryCnt int64) error {
	if retryCnt <= 0 {
		return errors.New("retriesCnt must be greater than zero")
	}
	updateQuery := `
		UPDATE public.queue
		SET status=$1,
			attempts_left = $2,
			expires_at = $6
		WHERE kind = $3 AND status in ($4, $5)
	`

	expiresAt := time.Now().Add(640 * time.Second)

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		status.OpenMustRetry,        // $1
		retryCnt,                    // $2 attempts
		kind,                        // $3
		status.ClosedExpired,        // $4 in
		status.ClosedNoAttemptsLeft, // $5 in
		expiresAt,                   // $6
	)

	return err
}

type dbSchedule struct {
	ID          int64         `db:"id"`
	Cron        string        `db:"cron"`
	Payload     []byte        `db:"payload"`
	MaxAttempts sql.NullInt16 `db:"max_attempts"`
	TTLSeconds  sql.NullInt32 `db:"ttl_seconds"`
}

func (s *sqlStorage) upsertSchedule(ctx context.Context, kind int16, name string, cronExpr string, payload []byte,
	maxAttempts uint16, ttlSeconds uint32, nextFireAt time.Time) error {

	upsertQuery := `
		INSERT INTO public.queue_schedule (kind, name, cron, payload, max_attempts, ttl_seconds, next_fire_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (kind, name) DO UPDATE SET
			cron = excluded.cron,
			payload = excluded.payload,
			max_attempts = excluded.max_attempts,
			ttl_seconds = excluded.ttl_seconds,
			enabled = true,
			updated = current_timestamp,
			next_fire_at = CASE WHEN queue_schedule.cron IS DISTINCT FROM excluded.cron
			                    THEN excluded.next_fire_at
			                    ELSE queue_schedule.next_fire_at END
	`

	var nullableMaxAttempts sql.NullInt16
	if maxAttempts != 0 {
		nullableMaxAttempts.Valid = true
		nullableMaxAttempts.Int16 = int16(maxAttempts)
	}

	var nullableTTLSeconds sql.NullInt32
	if ttlSeconds != 0 {
		nullableTTLSeconds.Valid = true
		nullableTTLSeconds.Int32 = int32(ttlSeconds)
	}

	_, err := s.db.ExecContext(
		ctx,
		upsertQuery,
		kind,                // $1
		name,                // $2
		cronExpr,            // $3
		string(payload),     // $4
		nullableMaxAttempts, // $5
		nullableTTLSeconds,  // $6
		nextFireAt,          // $7
	)
	return err
}

func (s *sqlStorage) deleteSchedule(ctx context.Context, kind int16, name string) error {
	deleteQuery := `
		DELETE FROM public.queue_schedule
		WHERE kind = $1 AND name = $2
	`

	_, err := s.db.ExecContext(ctx, deleteQuery, kind, name)
	return err
}

// spawnDueScheduledTasks publishes an ordinary task for every schedule of the
// given kind whose next_fire_at has passed, and moves that schedule on to its
// next fire. Both happen in one transaction under the schedule row lock, so
// concurrent processors cannot spawn the same fire twice.
//
// A fire that comes due while the previous task of the same schedule is still
// open is skipped rather than queued behind it; the returned counters report how
// many tasks were published and how many fires were skipped.
func (s *sqlStorage) spawnDueScheduledTasks(ctx context.Context, kind int16, maxAttempts uint16, ttlSeconds uint32,
	limit uint16) (int, int, error) {

	if limit == 0 {
		return 0, 0, errors.New("wrong limit")
	}

	selectQuery := `
		SELECT id, cron, payload, max_attempts, ttl_seconds
		FROM public.queue_schedule
		WHERE kind = $1
		  AND enabled
		  AND next_fire_at <= $2
		ORDER BY next_fire_at
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`

	insertQuery := `
		INSERT INTO public.queue (kind, attempts_left, payload, expires_at, delayed_till, schedule_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (schedule_id) WHERE schedule_id IS NOT NULL AND status <= 3 DO NOTHING
	`

	advanceQuery := `
		UPDATE public.queue_schedule SET
			next_fire_at = $2,
			last_fire_at = $3,
			updated = $3
		WHERE id = $1
	`

	disableQuery := `
		UPDATE public.queue_schedule SET
			enabled = false,
			updated = $2
		WHERE id = $1
	`

	var spawned, skipped int

	txErr := s.withTransaction(ctx, "spawn due scheduled tasks", func(ctx context.Context, tx *sqlx.Tx) error {
		var (
			now      = time.Now()
			err      error
			res      sql.Result
			affected int64
		)

		var schedules []*dbSchedule
		err = tx.SelectContext(ctx, &schedules, selectQuery,
			kind,  // $1
			now,   // $2
			limit, // $3
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		for _, schedule := range schedules {
			nextFireAt, cronErr := nextCronFire(schedule.Cron, now)
			if cronErr != nil {
				// Only reachable for a row that bypassed ScheduleTask validation.
				// Disable it: a schedule that cannot say when it fires next would
				// otherwise stay due and spawn a task on every maintenance tick.
				logger.Errorf(ctx, "disabling schedule %d of kind %d: %v", schedule.ID, kind, cronErr)
				if _, err = tx.ExecContext(ctx, disableQuery, schedule.ID, now); err != nil {
					return err
				}
				continue
			}

			attemptsLeft := maxAttempts
			if schedule.MaxAttempts.Valid {
				attemptsLeft = uint16(schedule.MaxAttempts.Int16)
			}

			ttl := ttlSeconds
			if schedule.TTLSeconds.Valid {
				ttl = uint32(schedule.TTLSeconds.Int32)
			}

			res, err = tx.ExecContext(ctx, insertQuery,
				kind,                     // $1
				attemptsLeft,             // $2
				string(schedule.Payload), // $3
				now.Add(time.Duration(ttl)*time.Second), // $4
				now,         // $5
				schedule.ID, // $6
			)
			if err != nil {
				return err
			}

			affected, err = res.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				// The previous task of this schedule is still open.
				logger.Warnf(ctx, "skip fire of schedule %d of kind %d: previous task is still open", schedule.ID, kind)
				skipped++
			} else {
				spawned++
			}

			if _, err := tx.ExecContext(ctx, advanceQuery,
				schedule.ID, // $1
				nextFireAt,  // $2
				now,         // $3
			); err != nil {
				return err
			}
		}

		return nil
	})
	if txErr != nil {
		return 0, 0, txErr
	}

	return spawned, skipped, nil
}

type dbTask struct {
	ID           int64          `db:"id"`
	Kind         int16          `db:"kind"`
	Status       uint8          `db:"status"`
	AttemptsLeft uint16         `db:"attempts_left"`
	Payload      []byte         `db:"payload"`
	ExternalKey  sql.NullString `db:"external_key"`
	RepeatPeriod sql.NullInt32  `db:"repeat_period"`
}

func fromDBTask(t *dbTask) *Task {
	if t == nil {
		return nil
	}
	return &Task{
		ID:           t.ID,
		Kind:         t.Kind,
		Payload:      t.Payload,
		attemptsLeft: t.AttemptsLeft,
		ExternalKey:  t.ExternalKey.String,
		RepeatPeriod: uint32(t.RepeatPeriod.Int32),
	}
}

func fromDBTasks(input []*dbTask) []*Task {
	if input == nil {
		return nil
	}
	output := make([]*Task, len(input))
	for i, t := range input {
		output[i] = fromDBTask(t)
	}
	return output
}
