package pgqueue

import (
	"github.com/jmoiron/sqlx"
)

// SQLProcessor processes tasks from the queue and allows you to publish them in a transaction.
type SQLProcessor struct {
	*processor
}

// MakeSQLProcessor returns new task processor.
func MakeSQLProcessor(
	db *sqlx.DB,
) *SQLProcessor {
	return &SQLProcessor{
		&processor{
			kindData:         make(map[int16]kindDescription),
			storage:          newSQLStorage(db),
			metricsCollector: &NoMetricsCollector{},
		},
	}
}

// SetMetricsCollector sets metrics client which must implement MetricsCollector interface
func (qp *SQLProcessor) SetMetricsCollector(metricsCollector MetricsCollector) {
	qp.metricsCollector = metricsCollector
}
