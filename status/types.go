package status

// Status представляет статус задачи.
type Status int64

// Возможные статусы задач.
const (
	// OpenNew присваивается задаче при создании.
	OpenNew Status = 0
	// OpenMustRetry присваивается задаче после неудачной попытки выполнения,
	// если лимит попыток еще не исчерпан.
	OpenMustRetry Status = 1
	// OpenProcessing означает, что в данный момент задача выполняется
	// воркером.
	OpenProcessing Status = 2

	// OpenClosedStatusesDivide - значение разделяющее Open/Closed статусы.
	// Нужно для проверки status < OpenClosedStatusesDivide
	// При необходимости можно менять, но нельзя допускать пересечений со статусами.
	OpenClosedStatusesDivide = 3

	// ClosedCancelled означает, что задача отменена пользователем.
	ClosedCancelled Status = 95
	// ClosedAborted присваивается задаче, если пользовательский обработчик вернул
	// ошибку pgqueue.ErrMustAbortTask.
	ClosedAborted Status = 96
	// ClosedLost присваивается задаче, если очередная попытка выполнить ее не смогла
	// завершиться за AttemptLimitSeconds секунд, и попытки после этого закончились.
	ClosedLost Status = 97
	// ClosedExpired означает, что задача была принудительно завершена, так как
	// не смогла завершиться за TTLSeconds после создания.
	ClosedExpired Status = 98
	// ClosedNoAttemptsLeft означает, что задача завершилась с ошибкой максимально
	// допустимое число раз
	ClosedNoAttemptsLeft Status = 99
	// ClosedSuccess означает, что задача завершена успешно.
	ClosedSuccess Status = 100
)

// Desc возвращает строковое представление статуса.
func (s Status) Desc() string {
	return getName(s)
}

var (
	description = map[Status]string{
		OpenNew:        "new",
		OpenMustRetry:  "must_retry",
		OpenProcessing: "processing",

		ClosedCancelled:      "cancelled",
		ClosedAborted:        "aborted",
		ClosedLost:           "lost",
		ClosedExpired:        "expired",
		ClosedNoAttemptsLeft: "no_attempts",
		ClosedSuccess:        "success",
	}
)

func getName(st Status) string {
	desc, ok := description[st]
	if !ok {
		desc = "unknown"
	}
	return desc
}
