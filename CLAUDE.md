# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`pgqueue` (module `github.com/kasorse/pgqueue`) is a PostgreSQL-backed task queue library for Go. Tasks live in a `queue` table (schema in `migrations/`), get picked up by polling processors, and are handed to user-registered handlers. It provides at-least-once delivery, not exactly-once — see README.md for the full processing/retry/TTL semantics before changing any status-transition logic.

## Commands

```bash
go test -v -race -covermode atomic ./...   # run all tests (same as `make test`)
go test -run TestName ./...                 # run a single test

make lint          # golangci-lint using .golangci.pipeline.yaml (downloads pinned v1.49.0 into ./bin if needed)
make mock           # regenerate all minimock mocks (requires `minimock` installed)

make db-status DATABASE_DSN=...   # goose migration status (dir: migrations)
make db-up DATABASE_DSN=...       # apply migrations
make db-up-by-one DATABASE_DSN=...
make db-down DATABASE_DSN=...
make db-create NAME='migration name'   # scaffold a new goose migration in migrations/
```

The `example/` directory is a **separate Go module** (its own `go.mod`) demonstrating library usage; it is not part of the root module's build/test/lint.

## Architecture

### Core flow: one goroutine tree per registered task kind

`processor` (in `processor.go`) is the root type; `SQLProcessor` (`sql_processor.go`) wraps it and is what users construct via `MakeSQLProcessor(db)`. Each task `kind` (an `int16`) is registered independently via `RegisterKind(kind, handler, opts)`, and `processor.runLoop` spins up **three concurrent goroutines per kind**:

1. **`runMaintenanceLoop`** — ticks every ~1.3s, calls `processMaintenance`, which does four things in sequence: publishes tasks for due cron schedules (`spawnDueScheduledTasks`, see below), expires TTL'd tasks (`closeExpiredTasks`), un-sticks tasks stuck in "processing" past `AttemptLimitSeconds` (`repairLostTasks`), and hard-deletes success tasks older than 7 days (`archiveClosedTasks`).
2. **`runTaskFetcher`** — ticks every ~1.1s, checks `workerMonitor.getRestingCount()` (idle workers for this kind), and if >0 calls `storage.getTasks` to atomically claim that many rows (`UPDATE ... FOR UPDATE SKIP LOCKED`), pushing them into a buffered `tasks` channel.
3. **`runWorkerPool`** — `WorkerCount` goroutines (`worker.run`) reading from that channel, each executing the handler with a timeout of `AttemptLimitSeconds`, then calling `completeTask`/`refuseTask`/`abortTask` depending on outcome.

`workerMonitor` (`worker_monitor.go`) is a simple mutex-guarded counter of idle workers per kind — it's the backpressure mechanism between the fetcher and the pool; it is intentionally decoupled from the DB-side `ConcurrencyLimit` semaphore (`workerCountLimitForQueueKind` in `getTasks`), which caps total in-flight rows for a kind across *all* processor instances.

### Storage layer

All SQL lives behind the `storage` interface (defined in `processor.go`, implemented in `sql_storage.go`). Task fetching, completion, refusal, and maintenance queries all use `status.Status` constants (`status/types.go`) to move rows through the state machine:
`OpenNew(0) → OpenMustRetry(1) ⇄ OpenProcessing(2)` (open, `< OpenClosedStatusesDivide`), terminating in one of `ClosedCancelled(95)`, `ClosedAborted(96)`, `ClosedLost(97)`, `ClosedExpired(98)`, `ClosedNoAttemptsLeft(99)`, `ClosedSuccess(100)`.

Key mechanics to understand before touching `sql_storage.go`:
- `createTask`/`createTaskTx` use `ON CONFLICT (kind, external_key) WHERE status <= 3 DO NOTHING`, so appending a task whose `external_key` already has an open row is silently a no-op.
- `getTasks` runs inside a transaction and uses `FOR UPDATE SKIP LOCKED` so multiple processor instances can poll concurrently without blocking each other; if `ConcurrencyLimit` (`workerCountLimitForQueueKind`) is set, it first counts currently-`OpenProcessing` rows for the kind and shrinks the claim limit accordingly.
- `completeTask` always writes `ClosedSuccess` — there is no longer an `endlessly` flag that recycles a row back to `OpenMustRetry` (see the scheduling section below).
- `worker.processTask` (`worker.go`) computes the retry delay: `ProgressiveDelayAfterRefusedSeconds` (indexed by attempt number, clamped to the last element) takes precedence over the flat `DelayAfterRefusedSeconds`; a task-level `RepeatPeriod` overrides both — `RepeatPeriod` is only a per-task delay override now, not a recurrence mechanism.
- A handler returning `ErrMustAbortTask` (compared via `errors.Cause`, so it can be wrapped) short-circuits straight to `abortTask` regardless of attempts remaining.

### Cron scheduling

Recurring tasks are **not** long-lived `queue` rows. They live in their own
`queue_schedule` table (migration `00002`), keyed by `(kind, name)`, holding the
cron expression plus a precomputed `next_fire_at`. `processor.ScheduleTask` upserts
one (idempotent: an unchanged expression keeps its pending fire, a changed one
re-derives it); `UnscheduleTask` deletes it.

`spawnDueScheduledTasks` (`sql_storage.go`) is the whole engine, and runs in one
transaction per maintenance tick: `SELECT ... WHERE enabled AND next_fire_at <= now()
... FOR UPDATE SKIP LOCKED` (so concurrent processors can't double-fire), then per
row an ordinary `INSERT INTO queue (..., schedule_id)` and an `UPDATE` advancing
`next_fire_at` to `nextCronFire(cron, now)`. Insert and advance commit together, so
a crash mid-transaction just retries on the next tick.

Consequences worth keeping in mind before changing any of this:

- A published run is an **ordinary task row** — `getTasks`, `completeTask`,
  `refuseTask`, `repairLostTasks`, `closeExpiredTasks`, `archiveClosedTasks` and
  `worker.go` have no cron awareness at all. A failing run exhausts its attempts and
  closes; the schedule is untouched and fires again next time.
- Overlap-skip is enforced by the DB, not by Go: `queue_schedule_run_uidx` is a
  partial unique index on `queue (schedule_id) WHERE schedule_id IS NOT NULL AND
  status <= 3`, and the insert is `ON CONFLICT (schedule_id) ... DO NOTHING`, so
  `RowsAffected == 0` *means* "previous run still open, fire skipped". NULLs are
  distinct in a unique index, so directly-published tasks are unconstrained.
- `cron.go` is the only file importing `robfig/cron/v3`. `nextCronFire` must always
  be passed a **local** `time.Now()`: the timestamp columns are `without time zone`,
  and v3 evaluates a `CRON_TZ`-less spec in the input's location, so a UTC input
  would store a consistently wrong wall clock. It also rejects `Next`'s zero time
  (`0 0 30 2 *` parses but never fires) — storing that would leave the schedule
  permanently due.
- A schedule whose expression fails to parse at spawn time (only reachable by
  hand-editing the row) is set `enabled = false` rather than retried.

`Options.RepeatEndlessly` and the `endlessly` column were removed in favour of this.
The schema change is deliberately split across two migrations so a running service can
upgrade without downtime: `00002` only *adds* (`queue_schedule`, `queue.schedule_id`,
`CREATE OR REPLACE VIEW` keeping `endlessly`) and is backward compatible with v0.1.x,
`00003` drops `endlessly`. Keep that property when editing them — anything the old code
still reads belongs in `00003`, not `00002`, and never merge the two: consumers migrate
before deploying, so `00003` has to be held back for a release after the code ships or it
drops the column under still-running v0.1.x pods. UPGRADE.md documents the sequence and is
the thing to update if these migrations change.

### Handler wrapping

`wrap.go`'s `wrappedHandler` transparently wraps every registered `TaskHandler` to recover panics inside `HandleTask` and turn them into errors (with stack trace attached via `errors.Wrap`), so a panicking handler degrades to a normal retry/failure instead of crashing the worker goroutine.

### Mocks

`*_mock_test.go` files are generated by `gojuno/minimock` (`make mock`) against the `storage`, `workerMonitor`, `TaskHandler`, and `MetricsCollector` interfaces — do not hand-edit them; change the interface and regenerate instead.

### Logging

`internal/logger` wraps logrus with a context-carried prefix (`logger.WrapContext` + `logger.SetItems`) so log lines are tagged with processor name, worker ID, task ID/kind without threading them through every call. `processor.SetLogLevel`/`SetJSONLogFormat` on the public processor control this global logger.
