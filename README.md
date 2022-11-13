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
2. Implement a metrics collector that has the `pgqueue.MetricsCollector` interface.
3. Implement a task handler that has the `pgqueue.TaskHandler` interface.
4. Start the task processor in the main of your service.
5. Schedule tasks using the `AppendTask` method. If you need to schedule a task 
in a transaction use the `SQLAppendTask` method.

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
