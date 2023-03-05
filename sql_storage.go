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

func (s *sqlStorage) withTransaction(ctx context.Context, name string, fn txFunc) error {
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
	ttlSeconds uint32, externalKey string, delay time.Duration, endlessly bool) error {

	insertQuery := `
		INSERT INTO public.queue (kind, attempts_left, endlessly, payload, expires_at, external_key, delayed_till)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	delayedTill := time.Now().Add(delay)
	expiresAt := delayedTill.Add(time.Duration(ttlSeconds) * time.Second)

	var nullableExternalKey *string
	if externalKey != "" {
		nullableExternalKey = &externalKey
	}

	_, err := s.db.ExecContext(ctx, insertQuery, kind, maxAttempts, endlessly, string(payload), expiresAt, nullableExternalKey, delayedTill)
	return err
}

func (s *sqlStorage) createTaskTx(ctx context.Context, tx sqlx.Tx, kind int16, maxAttempts uint16, payload []byte,
	ttlSeconds uint32, externalKey string, delay time.Duration, endlessly bool) error {
	insertQuery := `
	INSERT INTO public.queue (kind, attempts_left, endlessly, payload, expires_at, external_key, delayed_till)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
`

	delayedTill := time.Now().Add(delay)
	expiresAt := delayedTill.Add(time.Duration(ttlSeconds) * time.Second)

	var nullableExternalKey *string
	if externalKey != "" {
		nullableExternalKey = &externalKey
	}

	_, err := tx.ExecContext(ctx, insertQuery, kind, maxAttempts, endlessly, string(payload), expiresAt, nullableExternalKey, delayedTill)
	return err
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
			attempts_left = CASE WHEN not endlessly THEN attempts_left - 1 ELSE attempts_left END,
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
		RETURNING id, kind, attempts_left, payload, external_key
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

func (s *sqlStorage) completeTask(ctx context.Context, id int64, delaySeconds uint32) error {
	updateQuery := `
		UPDATE public.queue SET
			status = CASE WHEN not endlessly THEN $2::smallint ELSE $4::smallint END,
			updated = $3,
			delayed_till = $5
		WHERE id = $1
	`

	now := time.Now()
	delayedTill := now.Add(time.Duration(delaySeconds) * time.Second)

	_, err := s.db.ExecContext(
		ctx,
		updateQuery,
		id,                   // $1
		status.ClosedSuccess, // $2
		time.Now(),           // $3
		status.OpenMustRetry, // $4
		delayedTill,          // $5
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
		  AND NOT endlessly
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
			status = CASE WHEN attempts_left > 0 OR endlessly is true THEN $4::smallint ELSE $5::smallint END
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

type dbTask struct {
	ID           int64          `db:"id"`
	Kind         int16          `db:"kind"`
	Status       uint8          `db:"status"`
	AttemptsLeft uint16         `db:"attempts_left"`
	Payload      []byte         `db:"payload"`
	ExternalKey  sql.NullString `db:"external_key"`
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
