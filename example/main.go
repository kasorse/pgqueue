package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kasorse/pgqueue"
	_ "github.com/lib/pq"
)

const (
	myTaskKind      int16 = 42
	myScheduledKind int16 = 43
)

func main() {
	//logger.SetLevel(zap.DebugLevel)
	ctx := context.Background()
	pg, err := connectToPg(ctx)
	if err != nil {
		log.Fatalf("cannot connect to postgresql: %v", err)
	}
	processor := pgqueue.MakeSQLProcessor(pg)
	processor.SetMetricsCollector(&MetricsCollector{})
	processor.RegisterKind(myTaskKind, &TaskHandler{}, pgqueue.Options{
		Name:                     "my_processor",
		WorkerCount:              10,
		MaxAttempts:              0,
		AttemptLimitSeconds:      10,
		DelayAfterRefusedSeconds: 5,
	})
	processor.RegisterKind(myScheduledKind, &TaskHandler{}, pgqueue.Options{
		Name:                     "my_scheduled_processor",
		WorkerCount:              1,
		MaxAttempts:              3,
		AttemptLimitSeconds:      10,
		DelayAfterRefusedSeconds: 5,
	})
	processorCtx, processorCancel := context.WithCancel(ctx)
	processorDone, err := processor.Start(processorCtx)
	if err != nil {
		log.Fatalf("cannot start processor: %v", err)
	}

	// The processor publishes this task itself every 10 seconds. Use a cron
	// expression such as "0 0 * * *" for a wall-clock schedule instead. The call
	// is idempotent on (kind, name), so running it on every start is fine.
	schedulePayload, err := json.Marshal(MyTask{Foobar: "scheduled"})
	if err != nil {
		log.Fatalf("cannot encode scheduled payload: %v", err)
	}
	err = processor.ScheduleTask(ctx, myScheduledKind, "every_ten_seconds", "@every 10s", schedulePayload, nil)
	if err != nil {
		log.Fatalf("cannot schedule task: %v", err)
	}
	httpHandler := &HTTPHandler{processor: processor}
	http.HandleFunc("/append_task", httpHandler.AppendTask)
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Printf("finished serving: %v", err)
	}
	processorCancel()
	<-processorDone
}

type HTTPHandler struct {
	processor *pgqueue.SQLProcessor
}

func (h *HTTPHandler) AppendTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	queryValues := r.URL.Query()
	task := MyTask{Foobar: queryValues.Get("foobar")}
	payload, err := json.Marshal(task)
	if err != nil {
		log.Printf("error encoding task payload: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ctx := context.Background()
	err = h.processor.AppendTask(ctx, myTaskKind, payload)
	if err != nil {
		log.Printf("error appending task: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type MyTask struct {
	Foobar string
}

type TaskHandler struct {
}

func (t *TaskHandler) HandleTask(ctx context.Context, task *pgqueue.Task) error {
	var payload MyTask
	err := json.Unmarshal(task.Payload, &payload)
	if err != nil {
		return fmt.Errorf("cannot unmarshal task payload: %w", err)
	}
	log.Printf("executing task %#v with payload %#v", task, payload)
	if rand.Float64() < 0.5 {
		return fmt.Errorf("boom")
	}
	return nil
}

type MetricsCollector struct {
}

func (m *MetricsCollector) IncBusyWorkers(processorName string, taskKind int16) {
}

func (m *MetricsCollector) DecBusyWorkers(processorName string, taskKind int16) {
}

func (m *MetricsCollector) MeasureTaskDuration(
	processorName string,
	taskKind int16,
	isSuccessful bool,
	duration time.Duration,
) {
}

func connectToPg(ctx context.Context) (*sqlx.DB, error) {
	dsn, _ := os.LookupEnv("PG_DSN")
	if len(dsn) == 0 {
		return nil, fmt.Errorf("DSN is not specified")
	}

	db, err := sqlx.ConnectContext(ctx, "postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open PostgreSQL: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("cannot connect to PostgreSQL: %w", err)
	}
	return db, nil
}
