-- +goose Up
-- +goose StatementBegin
CREATE TABLE queue_status
(
    id   smallint PRIMARY KEY NOT NULL,
    name text                 NOT NULL
);

ALTER TABLE queue_status
    ADD CONSTRAINT name_size CHECK (LENGTH(name) <= 100) NOT VALID;
ALTER TABLE queue_status
    VALIDATE CONSTRAINT name_size;
--nolint:disallowed-unique-constraint
ALTER TABLE queue_status
    ADD CONSTRAINT queue_status_id_name_unique UNIQUE (id, name);
--nolint:disallowed-unique-constraint
ALTER TABLE queue_status
    ADD CONSTRAINT queue_status_name_unique UNIQUE (name);

INSERT INTO queue_status (id, name)
SELECT *
FROM unnest(
        array [
            0, 1, 2,
            95, 96, 97, 98,
            99, 100
            ],
        array [
            'new', 'must_retry', 'processing',
            'cancelled', 'aborted', 'lost', 'expired',
            'no_attempts', 'success'
            ]
    )
ON CONFLICT DO NOTHING;

CREATE TABLE queue
(
    id            bigserial PRIMARY KEY                                          NOT NULL,
    kind          smallint                                                       NOT NULL,
    status        smallint REFERENCES queue_status (id) DEFAULT 0                NOT NULL,
    attempts_left smallint                              DEFAULT 1                NOT NULL,
    endlessly     bool                                  DEFAULT false            NOT NULL,
    payload       jsonb                                                          NOT NULL,
    created       timestamp without time zone           DEFAULT now()            NOT NULL,
    updated       timestamp without time zone           DEFAULT now()            NOT NULL,
    delayed_till  timestamp without time zone           DEFAULT now()            NOT NULL,
    expires_at    timestamp without time zone                                    NOT NULL,
    messages      text[]                                DEFAULT ARRAY []::text[] NOT NULL,
    external_key  text,
    repeat_period int check (repeat_period >= 0)
);

COMMENT ON COLUMN queue.status IS '0: never processed, 1: must try one or more attempts, 2: now in processing, >50: closed for any reasons';
COMMENT ON COLUMN queue.attempts_left IS 'if 0 attempts remain, task must be closed';
COMMENT ON COLUMN queue.updated IS 'moment when task status was updated';
COMMENT ON COLUMN queue.delayed_till IS 'moment when task can be obtained for processing';
COMMENT ON COLUMN queue.expires_at IS 'moment when task must be closed without processing';
COMMENT ON COLUMN queue.messages IS 'reasons of processing failures';

--nolint:require-concurrent-index-creation
CREATE INDEX queue_external_key_idx ON queue USING btree (external_key) WHERE (external_key IS NOT NULL);
--nolint:require-concurrent-index-creation
CREATE INDEX queue_kind_status_delayed_till_attempts_left_idx ON queue USING btree (kind, status, delayed_till, attempts_left);
--nolint:require-concurrent-index-creation
CREATE INDEX queue_kind_status_expires_at_idx ON queue USING btree (kind, status, expires_at);
--nolint:require-concurrent-index-creation
CREATE INDEX queue_kind_status_updated_idx ON queue USING btree (kind, status, updated);
--nolint:require-concurrent-index-creation
CREATE UNIQUE INDEX queue_kind_external_key_uidx ON queue (kind, external_key) where status <= 3;

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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW queue_tasks_board;
DROP TABLE queue;
DROP TABLE queue_status;
-- +goose StatementEnd
