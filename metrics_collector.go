package pgqueue

import "time"

// MetricsCollector describes metrics collector interface.
type MetricsCollector interface {
	// IncBusyWorkers is called when the worker receives a new task.
	IncBusyWorkers(processorName string, taskKind int16)
	// DecBusyWorkers is called at the very end of task processing.
	DecBusyWorkers(processorName string, taskKind int16)
	// MeasureTaskDuration is called after the custom task handler function is executed.
	MeasureTaskDuration(
		processorName string,
		taskKind int16,
		isSuccessful bool,
		duration time.Duration,
	)
}

type NoMetricsCollector struct {
}

func (m *NoMetricsCollector) IncBusyWorkers(processorName string, taskKind int16) {
}

func (m *NoMetricsCollector) DecBusyWorkers(processorName string, taskKind int16) {
}

func (m *NoMetricsCollector) MeasureTaskDuration(
	processorName string,
	taskKind int16,
	isSuccessful bool,
	duration time.Duration,
) {
}
