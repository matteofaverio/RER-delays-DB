CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.delay_events (
    poll_at_utc       TIMESTAMPTZ NOT NULL,
    poll_at_local     TIMESTAMPTZ NOT NULL,
    stop_id           TEXT NOT NULL,
    line_code         TEXT NOT NULL,
    mean_delay_s      DOUBLE PRECISION NOT NULL,
    mean_lateness_s   DOUBLE PRECISION NOT NULL,
    n                 INTEGER NOT NULL CHECK (n >= 0),
    n_neg             INTEGER NOT NULL CHECK (n_neg >= 0),
    n_pos             INTEGER NOT NULL CHECK (n_pos >= 0),
    source_file       TEXT NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);