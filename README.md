Task queue on PostgreSQL

Documentation could be found in code or with godoc:
```bash
go install golang.org/x/tools/cmd/godoc@v0.1.9
godoc -http localhost:6060
# After open http://localhost:6060/pkg/github.com/kasorse/pgqueue/ link
```


## Idea

pgqueue allows you to process tasks that are stored in a `queue` table, which 
is pre-created by the service owners. The task can contain a custom payload in 
JSON format. Processing errors are rerun for the configured number of retries.

The user creates a **task processor** in the main of his service and for each 
type of task registers a **handler** — a structure with a method that accepts 
a task and returns an error. The user then schedules the tasks in a transaction. 
The task processor in a loop selects tasks from the table, sets them to the 
"processing" status and gives the tasks to the pool of **workers** that run 
registered custom handlers. A successfully completed task is marked in the table 
with the corresponding status.

pgqueue provides an at-least-once guarantee, but no exactly once guarantee, and 
it is also possible for two workers to run the same task concurrently 
(see [section on handling lost tasks](#handling-lost-tasks)).

## How to use

1. Copy to your service migrations from the [migrations](migrations) folder and run them.
If you are upgrading an already-running service from v0.1.x, follow [UPGRADE.md](UPGRADE.md) 
instead of applying them all at once.
2. Implement a metrics collector that has the `pgqueue.MetricsCollector` interface.
3. Implement a task handler that has the `pgqueue.TaskHandler` interface.
4. Start the task processor in the main of your service.
5. Schedule tasks using the `AppendTask` method. If you need to schedule a task 
in a transaction use the `SQLAppendTask` method.
6. For recurring tasks, declare a cron schedule with the `ScheduleTask` method — 
see the [scheduled tasks section](#scheduled-tasks).

See the tiny example in the [example](example) folder.

### Important settings

When creating a processor, it is recommended to pay attention to the following 
settings:
1. `MaxAttempts` - the number of attempts to process the task. The default is 1, 
that is, after the first error, the task will end and will not be retried. It is 
necessary to set the largest number that is reasonable for this type of task.
2. `AttemptLimitSeconds` - task execution timeout. If the task runs longer, it will 
return to the queue, and another worker can start executing it. Therefore, the 
timeout must be large enough so that the expected execution time of the task fits 
within it. At the same time, if the timeout is too long, it will increase the time 
it takes to complete a task that hits the task processor on the dead pod.
3. `DelayAfterRefusedSeconds` / `ProgressiveDelayAfterRefusedSeconds` - delay before 
the next execution attempt. See the [error handling section](#error-handling) for details.

## Scheduled tasks

A task can be published by the processor itself on a wall-clock schedule written 
as a standard cron expression. Register the kind as usual, then declare the 
schedule:

```go
processor.RegisterKind(reportKind, &ReportHandler{}, pgqueue.Options{
	Name:                "reports",
	WorkerCount:         2,
	MaxAttempts:         3,
	AttemptLimitSeconds: 60,
})

// every day at 00:00
err := processor.ScheduleTask(ctx, reportKind, "nightly_report", "0 0 * * *", payload, nil)
```

`ScheduleTask` is idempotent on `(kind, name)`, so it is meant to be called on 
every service start: an unchanged expression keeps its pending fire, a changed one 
re-derives the next fire immediately. `UnscheduleTask(ctx, kind, name)` removes a 
schedule; a task it has already published is left to finish.

Schedules live in the `queue_schedule` table and can be inspected there directly. 
The `ScheduleOptions` argument (`nil` above) overrides `MaxAttempts` and 
`TTLSeconds` for the published tasks; zero values inherit from the kind.

### Supported expressions

`ScheduleTask` accepts everything [robfig/cron](https://pkg.go.dev/github.com/robfig/cron/v3) 
parses in standard mode:

| expression | meaning |
|---|---|
| `0 0 * * *` | every day at 00:00 |
| `*/15 * * * *` | every 15 minutes |
| `30 6 * * 1-5` | at 06:30 on weekdays |
| `@daily`, `@hourly`, `@weekly` | descriptors |
| `@every 90s`, `@every 2h30m` | fixed intervals |
| `CRON_TZ=Europe/Moscow 0 0 * * *` | midnight in an explicit timezone |

Without a `CRON_TZ` prefix the expression is evaluated in the local timezone of 
the process. Use `pgqueue.ValidateCronSchedule` to check an expression ahead of 
time; both it and `ScheduleTask` reject an expression that cannot be parsed or 
that can never fire (`0 0 30 2 *`), returning an error wrapping 
`pgqueue.ErrInvalidCronSchedule`.

### Semantics

A published task is an **ordinary task row**: it retries with the configured 
backoff, respects `MaxAttempts` and TTL, and ends in one of the usual statuses. 
A failing run therefore never blocks the schedule — once the attempts are spent 
the task closes, and the schedule fires again at its next scheduled moment. 
Three further properties:

- **A schedule never fires at the moment it is created.** The first task is 
published the next time the expression matches, so a `0 0 * * *` schedule 
declared at 09:00 first runs at midnight.
- **A missed fire is caught up once.** The next fire is computed from the current 
time, so an outage spanning ten scheduled moments produces one late task, not ten.
- **An overlapping fire is skipped.** If a schedule comes due while its previous 
task is still open, that fire is dropped and logged; there is at most one open 
task per schedule.

Fires are detected by the same maintenance loop that expires and repairs tasks, 
which ticks roughly every 1.3 seconds — far finer than cron's one-minute 
resolution, but a fire may be published a moment after its scheduled time.

## Upgrading

### From v0.1.x: `RepeatEndlessly` is replaced by schedules

`Options.RepeatEndlessly` and the `endlessly` column are gone; cron schedules 
cover that functionality without the drift of an interval measured from the end 
of the previous run. Translate each endless kind into a schedule:

```go
// before
processor.RegisterKind(kind, handler, pgqueue.Options{RepeatEndlessly: true, /* ... */})
processor.AppendTaskWithOptions(ctx, kind, payload, nil, &pgqueue.AppendTaskOptions{RepeatPeriod: 30})

// after
processor.RegisterKind(kind, handler, pgqueue.Options{/* ... */})
processor.ScheduleTask(ctx, kind, "my_recurring_task", "@every 30s", payload, nil)
```

Rows that were `endlessly = true` finish their current run and then close 
normally; re-declare them with `ScheduleTask`. `AppendTaskOptions.RepeatPeriod` 
still exists, but now only overrides the retry delay of a single task.

The schema change is split so that a running service can be upgraded without 
downtime: migration `00002` only adds `queue_schedule` and `queue.schedule_id` 
and is backward compatible with v0.1.x, while `00003` drops `endlessly` and must 
be applied only after every instance runs the new version.

If your pipeline runs migrations before the new code goes live — most do — apply 
`00002` only in the release that ships this version (`goose ... up-to 2`, or copy 
just `00002` into your project), and leave `00003` for a later release. A plain 
`goose up` would apply both and break the pods that are still serving. 
**[UPGRADE.md](UPGRADE.md) walks through the whole sequence**, including 
deployment order, locking, mixed-version safety and rollback.

## Error handling

Custom handler errors are retried for `MaxAttempts` times (default 1). If the attempts 
are over, the task exits with status 99 (`ClosedNoAttemptsLeft`). Using the `AddRetries` 
method, you can add attempts to all such tasks for the selected task type.

The delay before the next processing attempt is set by the `DelayAfterRefusedSeconds` 
and `ProgressiveDelayAfterRefusedSeconds` options. The `DelayAfterRefusedSeconds` 
option specifies a constant delay in seconds, the `ProgressiveDelayAfterRefusedSeconds` 
option  specifies a sequence of delays in seconds and takes precedence over 
`DelayAfterRefusedSeconds`.

## Handling lost tasks

It is possible that the task processor moved the task to the "processing" status, 
but then died or hung. To handle this case, the `AttemptLimitSeconds` timeout is 
imposed on one iteration of task processing. The task processor in a separate goroutine 
periodically finds tasks that run longer than the limit and returns them to the queue.

Thus, if the processor started to execute the task and immediately after that the 
pod died, then the processor on some other pod will return the task to the queue after 
`AttemptLimitSeconds` seconds, after which it will be picked up by any living processor.

## TTL

If a task has not completed within `TTLSeconds` seconds after creation, then it is set 
to status 98 (`ClosedExpired`). Using the `AddRetries` method, you can reset the number 
of attempts for all such tasks for the selected task type, after which they will return 
to the queue.

If the `TTLSeconds` option is not set, then it is calculated as the number of seconds 
that will be required for all `MaxAttempts` attempts, taking into account the settings 
`AttemptLimitSeconds` and `DelayAfterRefusedSeconds` / `ProgressiveDelayAfterRefusedSeconds`.
