-- Contract step: removes the column that pgqueue v0.1.x needs. Apply it only
-- after every instance runs a version with cron schedules, and after any
-- remaining endlessly rows have been re-declared with ScheduleTask. Running it
-- while an old instance is still polling makes that instance fail every query
-- against the queue table. See UPGRADE.md.

-- +goose Up
-- +goose StatementBegin
-- DROP + CREATE rather than REPLACE: replace cannot drop a column.
DROP VIEW queue_tasks_board;
ALTER TABLE queue
    DROP COLUMN endlessly;
CREATE VIEW queue_tasks_board AS
SELECT q.id,
       q.kind,
       q.status AS status_code,
       s.name   AS status_desc,
       q.attempts_left,
       q.schedule_id,
       q.payload,
       q.created,
       q.updated,
       q.delayed_till,
       q.expires_at
FROM (queue q
         JOIN queue_status s ON ((s.id = q.status)));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- endlessly comes back empty: tasks that were endlessly = true before the
-- upgrade do not resume recurring on their own, they must be re-published.
DROP VIEW queue_tasks_board;
ALTER TABLE queue
    ADD COLUMN endlessly bool DEFAULT false NOT NULL;
CREATE VIEW queue_tasks_board AS
SELECT q.id,
       q.kind,
       q.status AS status_code,
       s.name   AS status_desc,
       q.attempts_left,
       q.endlessly,
       q.payload,
       q.created,
       q.updated,
       q.delayed_till,
       q.expires_at,
       q.schedule_id
FROM (queue q
         JOIN queue_status s ON ((s.id = q.status)));
-- +goose StatementEnd
