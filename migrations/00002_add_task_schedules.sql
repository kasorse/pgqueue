-- Expand step: everything here is backward compatible with pgqueue v0.1.x, which
-- knows nothing about queue_schedule or queue.schedule_id and still needs
-- queue.endlessly. Apply this while the old version is running, deploy the new
-- version, and only then apply 00003, which drops endlessly. See UPGRADE.md.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE queue_schedule
(
    id           bigserial PRIMARY KEY                     NOT NULL,
    kind         smallint                                  NOT NULL,
    name         text                                      NOT NULL,
    cron         text                                      NOT NULL,
    payload      jsonb                                     NOT NULL,
    enabled      bool                        DEFAULT true  NOT NULL,
    max_attempts smallint,
    ttl_seconds  int,
    next_fire_at timestamp without time zone               NOT NULL,
    last_fire_at timestamp without time zone,
    created      timestamp without time zone DEFAULT now() NOT NULL,
    updated      timestamp without time zone DEFAULT now() NOT NULL
);

COMMENT ON COLUMN queue_schedule.name IS 'identifies the schedule within its kind, own namespace, unrelated to queue.external_key';
COMMENT ON COLUMN queue_schedule.cron IS 'standard five-field cron spec, @descriptor or @every interval';
COMMENT ON COLUMN queue_schedule.enabled IS 'disabled schedules are skipped; set to false when the spec becomes unusable';
COMMENT ON COLUMN queue_schedule.max_attempts IS 'attempts for the spawned task, NULL: inherit from the kind';
COMMENT ON COLUMN queue_schedule.ttl_seconds IS 'TTL for the spawned task, NULL: inherit from the kind';
COMMENT ON COLUMN queue_schedule.next_fire_at IS 'moment when the next task must be spawned';
COMMENT ON COLUMN queue_schedule.last_fire_at IS 'moment when a task was spawned last time';

ALTER TABLE queue_schedule
    ADD CONSTRAINT name_size CHECK (LENGTH(name) <= 100) NOT VALID;
ALTER TABLE queue_schedule
    VALIDATE CONSTRAINT name_size;
ALTER TABLE queue_schedule
    ADD CONSTRAINT cron_size CHECK (LENGTH(cron) <= 100) NOT VALID;
ALTER TABLE queue_schedule
    VALIDATE CONSTRAINT cron_size;
ALTER TABLE queue_schedule
    ADD CONSTRAINT max_attempts_positive CHECK (max_attempts IS NULL OR max_attempts > 0) NOT VALID;
ALTER TABLE queue_schedule
    VALIDATE CONSTRAINT max_attempts_positive;
ALTER TABLE queue_schedule
    ADD CONSTRAINT ttl_seconds_positive CHECK (ttl_seconds IS NULL OR ttl_seconds > 0) NOT VALID;
ALTER TABLE queue_schedule
    VALIDATE CONSTRAINT ttl_seconds_positive;

--nolint:require-concurrent-index-creation
CREATE UNIQUE INDEX queue_schedule_kind_name_uidx ON queue_schedule (kind, name);
--nolint:require-concurrent-index-creation
CREATE INDEX queue_schedule_kind_next_fire_at_idx ON queue_schedule (kind, next_fire_at) WHERE enabled;

-- Adding a nullable column with no default is metadata-only. The foreign key is
-- added NOT VALID and validated separately so that VALIDATE takes only
-- SHARE UPDATE EXCLUSIVE on queue instead of blocking the polling loops.
ALTER TABLE queue
    ADD COLUMN schedule_id bigint;
ALTER TABLE queue
    ADD CONSTRAINT queue_schedule_id_fkey FOREIGN KEY (schedule_id)
        REFERENCES queue_schedule (id) ON DELETE SET NULL NOT VALID;
ALTER TABLE queue
    VALIDATE CONSTRAINT queue_schedule_id_fkey;

COMMENT ON COLUMN queue.schedule_id IS 'schedule that spawned this task, NULL for tasks published directly';

-- at most one open task per schedule: a fire that comes due while the previous
-- run is still open is skipped. NULLs are distinct, so tasks published directly
-- are unaffected. On a large queue table build this CONCURRENTLY instead, see
-- UPGRADE.md.
--nolint:require-concurrent-index-creation
CREATE UNIQUE INDEX queue_schedule_run_uidx ON queue (schedule_id) WHERE schedule_id IS NOT NULL AND status <= 3;

-- REPLACE rather than DROP + CREATE: the new column can only be appended, but
-- endlessly stays readable for the old version and the view is never missing.
CREATE OR REPLACE VIEW queue_tasks_board AS
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

-- +goose Down
-- +goose StatementBegin
DROP VIEW queue_tasks_board;
DROP INDEX queue_schedule_run_uidx;
ALTER TABLE queue
    DROP COLUMN schedule_id;
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
       q.expires_at
FROM (queue q
         JOIN queue_status s ON ((s.id = q.status)));
DROP TABLE queue_schedule;
-- +goose StatementEnd
