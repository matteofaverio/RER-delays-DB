CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.dim_time (
    poll_at_utc    TIMESTAMPTZ PRIMARY KEY,
    poll_at_local  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_station (
    stop_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS core.dim_line (
    line_code TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS core.fact_delay_events (
    poll_at_utc      TIMESTAMPTZ NOT NULL REFERENCES core.dim_time(poll_at_utc),
    stop_id          TEXT NOT NULL REFERENCES core.dim_station(stop_id),
    line_code        TEXT NOT NULL REFERENCES core.dim_line(line_code),
    mean_delay_s     DOUBLE PRECISION NOT NULL,
    mean_lateness_s  DOUBLE PRECISION NOT NULL,
    n                INTEGER NOT NULL CHECK (n >= 0),
    n_neg            INTEGER NOT NULL CHECK (n_neg >= 0),
    n_pos            INTEGER NOT NULL CHECK (n_pos >= 0),
    PRIMARY KEY (poll_at_utc, stop_id, line_code),
    CHECK (n_neg + n_pos <= n),
    CHECK (mean_lateness_s >= 0)
);