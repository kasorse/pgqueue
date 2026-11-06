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

// KindInitializer is an optional extension of MetricsCollector. When the collector set with
// SetMetricsCollector also implements it, Start calls InitKind once for every registered task kind,
// before any task of that kind is processed.
//
// It lets a collector create its series at zero when the process starts. Without it a series first
// appears when MeasureTaskDuration reports the first finished task, so in a process that runs a task
// once (a daily job in a pod replaced about daily) a counter's first sample is already 1, and
// increase() over it reads 0: an alert on "no successful run in the last day" cannot tell a missing
// run from a new process. A run killed with its process, or never started, records no sample at all,
// which only such an alert can catch. Seed every value the collector later reports, for example both
// success="true" and success="false", or the first sample of the other one hits the same problem.
type KindInitializer interface {
	InitKind(processorName string, taskKind int16)
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
