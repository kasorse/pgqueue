# Upgrading to cron schedules with zero downtime

This describes moving a running service from pgqueue **v0.1.x** (`Options.RepeatEndlessly`
and the `queue.endlessly` column) to the version with [cron schedules](README.md#scheduled-tasks),
without pausing task publishing or processing and without a window in which any instance
is broken.

The upgrade is a standard **expand / migrate / contract** sequence, because the new code
and the old code disagree about one column:

- the new code needs `queue_schedule` and `queue.schedule_id`, which the old code ignores;
- the old code needs `queue.endlessly` in five statements (`createTask`, `getTasks`,
  `completeTask`, `closeExpiredTasks`, `repairLostTasks`), and the new code never
  references it.

So the additions can go in while the old version is still serving, but the removal cannot.
Migration `00002` contains only the additions and `00003` only the removal, and the two are
applied on either side of the deployment.

```
 1. apply 00002        additions only, old version keeps running
 2. deploy new version rolling; old and new instances run side by side
 3. declare schedules  ScheduleTask, normally from the new version's startup code
 4. drain endlessly    let the old rows close, or cancel them
 5. apply 00003        drops endlessly, after no old instance remains
```

Steps 1 and 5 are separate deployments' worth of time apart. Nothing forces them to be
close together — leaving days between step 2 and step 5 is the safe choice, since step 5
is the point after which rolling back to v0.1.x needs a migration too.

## Deployment order — read this first

Most pipelines run migrations as a step *before* the new code goes live. That order is
required for `00002` and fatal for `00003`:

- **`00002` before the deploy** — correct. The pods still serving are the old version, and
  `00002` is compatible with them.
- **`00003` before the deploy** — breaks production. It drops `endlessly` out from under
  pods that are still selecting it, and they fail every `queue` query until the new image
  replaces them.

So a migrate-then-deploy pipeline has exactly one rule: **`00003` must not be applied by the
same pipeline run that ships the new code.** The upgrade spans two releases.

```
release N     migrate up-to 00002  →  deploy new code      ← old pods survive the migration
              (steps 1-4)
release N+1   migrate (00003)      →  deploy (no changes)  ← pods already run the new code
              (step 5)
```

In release N+1 the migration still runs before the deploy, and that is fine: the pods running
at that moment are new-version pods, which never reference `endlessly`.

Two ways to hold `00003` back during release N, depending on how much of the migration step
you control:

1. **Pin the target** — `up-to 2` instead of `up` (see step 1). Needs a pipeline that accepts
   a version argument, and someone has to remember to unpin it later.
2. **Withhold the file** — if you copy `migrations/` into your own project, copy `00002` only
   and add `00003` in a later release. More robust when the pipeline hardcodes `goose up`,
   because nothing depends on remembering the pin.

`00003` is never urgent, and no state requires it: the new code does not reference `endlessly`,
and rows it inserts take the column's `DEFAULT false`. A database left permanently at `00002`
is fully functional, one dead boolean column wide. When in doubt, defer it.

## Step 1 — apply migration 00002

With the database at `00001`, apply exactly one migration:

```bash
make db-up-by-one DATABASE_DSN="..."
make db-status    DATABASE_DSN="..."   # 00002 applied, 00003 still pending
```

`make db-up` would apply `00003` too, which is precisely what must not happen yet — this is
the one thing to get right, see [Deployment order](#deployment-order--read-this-first). If
you drive `goose` directly, `up-to` is the explicit form:

```bash
goose -dir migrations postgres "$DATABASE_DSN" up-to 2
```

Everything in `00002` is backward compatible: a new nullable column, a new table, new
indexes, and a `CREATE OR REPLACE VIEW` that only appends `schedule_id` to
`queue_tasks_board` while keeping `endlessly`. The old version continues to work against
this schema unchanged, so this step is independently revertible (`make db-down`) and safe
to apply well before the deployment.

### Locking

`queue` is polled by every instance roughly once a second, so the two statements that need
a strong lock on it deserve care:

- `ALTER TABLE queue ADD COLUMN schedule_id bigint` needs `ACCESS EXCLUSIVE`, but only
  briefly: a nullable column with no default is a catalog change and does not rewrite the
  table. The foreign key is added `NOT VALID` and validated in a second statement, so
  validation takes only `SHARE UPDATE EXCLUSIVE` and never blocks the polling loops.
- `CREATE UNIQUE INDEX queue_schedule_run_uidx` takes `SHARE` on `queue`, which blocks
  writes — including `AppendTask` — for as long as the build takes. The index is partial
  and matches no rows yet, but building it still scans the heap.

Set a `lock_timeout` so a long-running transaction cannot turn a queued `ALTER` into a
pile-up of blocked queries behind it, and retry on timeout rather than waiting:

```sql
SET lock_timeout = '3s';
```

On a large `queue` table, build that index without blocking writes instead. Apply `00002`
with the index statement removed, then:

```sql
-- outside a transaction
CREATE UNIQUE INDEX CONCURRENTLY queue_schedule_run_uidx
    ON queue (schedule_id) WHERE schedule_id IS NOT NULL AND status <= 3;
```

Verify it before deploying — a `CONCURRENTLY` build that fails leaves an invalid index
behind, which would silently stop enforcing the overlap-skip rule:

```sql
SELECT indisvalid FROM pg_index WHERE indexrelid = 'queue_schedule_run_uidx'::regclass;
-- must be true; if false: DROP INDEX queue_schedule_run_uidx; and retry
```

## Step 2 — deploy the new version

Update the dependency and remove `RepeatEndlessly` from every `RegisterKind` call — it no
longer compiles:

```go
// before
processor.RegisterKind(kind, handler, pgqueue.Options{RepeatEndlessly: true, /* ... */})

// after
processor.RegisterKind(kind, handler, pgqueue.Options{/* ... */})
```

Deploy it as an ordinary rolling update. During the rollout old and new instances share the
database, which is safe in both directions:

- A task published by a new instance's schedule carries `schedule_id` and `endlessly = false`,
  so an old instance claims, runs and completes it as an ordinary task. Old instances never
  write `schedule_id`, and `NULL`s are distinct in a unique index, so `queue_schedule_run_uidx`
  does not constrain anything they publish.
- Only new instances run the spawner, so schedules start firing as soon as the first new
  instance is up, and fire at most once per due moment regardless of how many instances are
  new — `spawnDueScheduledTasks` locks the schedule row with `FOR UPDATE SKIP LOCKED` and
  advances `next_fire_at` in the same transaction as the insert.
- One behaviour does change mid-rollout: when a **new** instance completes an old
  `endlessly = true` task, it writes `ClosedSuccess` instead of returning the row to
  `OpenMustRetry`, so that task stops recurring at that point. This is what step 4 is about.

## Step 3 — declare the schedules

Translate each endless task into a schedule. `RepeatEndlessly` measured its period from the
end of the previous run; a cron schedule fires on the wall clock, so `@every 30s` is the
literal equivalent and `*/1 * * * *` or `0 * * * *` is usually what was actually meant:

```go
// before: an endless task re-published every 30 seconds
processor.AppendTaskWithOptions(ctx, kind, payload, nil, &pgqueue.AppendTaskOptions{RepeatPeriod: 30})

// after
processor.ScheduleTask(ctx, kind, "my_recurring_task", "@every 30s", payload, nil)
```

`ScheduleTask` is idempotent on `(kind, name)`, so the natural home for these calls is the
startup path that used to publish the endless tasks — running them on every boot neither
duplicates a schedule nor shifts its pending fire.

Note that **a schedule does not fire at the moment it is created**; the first task is
published at the next moment the expression matches. Between the last run of an endless task
and the first run of its schedule there is therefore a gap of up to one period. For the
second-scale periods `RepeatPeriod` was used for this is unnoticeable; if a specific job
cannot tolerate it, publish one task directly with `AppendTask` right after declaring the
schedule.

Confirm the schedules landed:

```sql
SELECT kind, name, cron, enabled, next_fire_at, last_fire_at FROM queue_schedule ORDER BY kind, name;
```

## Step 4 — drain the remaining endlessly rows

Find them:

```sql
SELECT id, kind, status, external_key, delayed_till FROM queue WHERE endlessly AND status <= 3;
```

Each of these rows is claimed once more by whichever instance picks it up. Under the new
version that run ends in `ClosedSuccess` and the row stops recurring by itself, so **doing
nothing is a valid choice** — every endless row drains within one of its own periods, and
the cost is one extra run per row.

If a duplicate run is not acceptable — the schedule and the leftover row could both fire
once in the same window — close them explicitly instead. Do this only after no old instance
is left, otherwise an old instance's `AppendTask` re-creates them:

```sql
UPDATE queue
SET status   = 95,
    updated  = now(),
    messages = array_append(messages, 'cancelled: migrated to queue_schedule')
WHERE endlessly AND status <= 3;
```

Status 95 is `ClosedCancelled`. Cancelling frees the row's `(kind, external_key)` slot in
`queue_kind_external_key_uidx` immediately, which matters if the schedule's tasks reuse that
key. A row already in `OpenProcessing` (status 2) is being executed right now; leave it out
of the `UPDATE` (`AND status <= 1`) if you would rather let it finish than have its result
land on a cancelled row.

Then confirm nothing is left before contracting:

```sql
SELECT COUNT(*) FROM queue WHERE endlessly AND status <= 3;   -- must be 0
```

## Step 5 — apply migration 00003

This belongs to a **later release than step 2**, never the same one — the migration step runs
while the previous release's pods are still serving, so those pods must already be on the new
version. Confirm it from your orchestrator (every replica of every service using this database
is on the new image, and no old-version job or cron container remains) rather than from the
database: there is no query that reveals a connected instance's code version.

What corroborates it is step 4's count staying at zero — only v0.1.x publishes
`endlessly = true`, so a row reappearing after you drained them means an old instance is still
out there.

```bash
make db-up DATABASE_DSN="..."
```

`00003` drops `endlessly` and rebuilds `queue_tasks_board` without it. Both statements are
catalog-only and take an `ACCESS EXCLUSIVE` lock on `queue` for a moment, so use the same
`lock_timeout` discipline as step 1.

If any instance is still running v0.1.x when this lands, it does not degrade gracefully — it
fails every query against `queue` with `column "endlessly" does not exist` until it is
replaced. Publishing and processing stop for that instance. That is the whole reason for the
split, and the reason this step waits for its own release.

## Rolling back

| you are at | to roll back |
|---|---|
| after step 1 | `make db-down` (reverses `00002`), or simply leave it: the old version runs fine against the expanded schema |
| after step 2, before step 5 | redeploy the old version. It finds `endlessly` intact and works immediately. Schedules stay in `queue_schedule`, dormant, since only the new version spawns from them. Tasks already spawned finish as ordinary tasks |
| after step 5 | `make db-down` re-adds `endlessly`, then redeploy the old version. The column comes back all-`false`, so previously endless tasks do **not** resume recurring — re-publish them with `RepeatEndlessly` |

The asymmetry is why step 5 should lag step 2 by however long you would want to keep a cheap
rollback available.

## Dashboards and other readers

`queue_tasks_board` is not read by the library, only by whatever you point at it. Its
`endlessly` column survives step 1 and disappears at step 5, replaced by `schedule_id`. If a
dashboard selects `endlessly` explicitly, update it between steps 2 and 5; `schedule_id` is
available from step 1 onward, and joining it to `queue_schedule` gives a per-schedule run
history for free:

```sql
SELECT s.name, b.status_desc, b.created, b.updated
FROM queue_tasks_board b JOIN queue_schedule s ON s.id = b.schedule_id
ORDER BY b.created DESC;
```
