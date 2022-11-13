package pgqueue

import (
	"time"

	"github.com/pkg/errors"
)

// установки по умолчанию
const (
	defaultWorkerCount uint16 = 1
	defaultMaxAttempts uint16 = 1

	emptyKey     string        = ""
	zeroDuration time.Duration = 0
)

var (
	// ErrMustAbortTask возвращается пользовательским обработчиком задачи,
	// если задачу необходимо закрыть, несмотря на оставшиеся попытки.
	ErrMustAbortTask = errors.New("according to business logic task must be closed with no retries")

	// ErrUnexpectedTaskKind возвращается процессором при попытке добавить в очередь
	// задание незарегистрированного типа.
	ErrUnexpectedTaskKind = errors.New("unexpected task kind")
)

// AppendTaskOptions описывает опции публикации задачи.
type AppendTaskOptions struct {
	// Ключ, с которым связана задача. Позволяет отменять задачи по ключу. Не
	// является уникальным.
	ExternalKey string
	// Задержка взятия задачи в работу.
	Delay time.Duration
}
