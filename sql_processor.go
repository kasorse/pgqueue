package pgqueue

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// SQLProcessor обрабатывает задачи из очереди и позволяет публиковать их в
// транзакции.
type SQLProcessor struct {
	*processor
}

// MakeSQLProcessor возвращает новый процессор задач.
func MakeSQLProcessor(
	db *sqlx.DB,
	metricsCollector MetricsCollector,
) *SQLProcessor {
	return &SQLProcessor{
		&processor{
			kindData:         make(map[int16]kindDescription),
			storage:          newSQLStorage(db),
			metricsCollector: metricsCollector,
		},
	}
}

// SQLAppendTask публикует задачу в очередь в переданной транзакции.
func (qp *SQLProcessor) SQLAppendTask(ctx context.Context, tx sqlx.Tx, kind int16, payload []byte) error {
	sqlStorage, ok := qp.storage.(*sqlStorage)
	if !ok {
		return errors.New("storage type casting error")
	}

	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	err := sqlStorage.createTaskTx(ctx, tx, kind, kindData.opts.MaxAttempts, payload, kindData.opts.TTLSeconds)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}

// SQLAppendTaskWithOptions публикует задачу в очередь в переданной транзакции.
// C помощью параметра opts можно переопределить для этой задачи максимальное
// количество попыток и TTL, в этом случае будет использовано только первое
// значение параметра.
func (qp *SQLProcessor) SQLAppendTaskWithOptions(ctx context.Context, tx sqlx.Tx, kind int16, payload []byte, opts ...Options) error {
	sqlStorage, ok := qp.storage.(*sqlStorage)
	if !ok {
		return errors.New("storage type casting error")
	}

	kindData, ok := qp.kindData[kind]
	if !ok {
		return ErrUnexpectedTaskKind
	}

	useOpts := kindData.opts
	if len(opts) > 0 {
		useOpts = opts[0]
	}

	err := sqlStorage.createTaskTx(ctx, tx, kind, useOpts.MaxAttempts, payload, useOpts.TTLSeconds)
	if err != nil {
		return errors.Wrap(err, "storage.createTask error")
	}

	return nil
}
