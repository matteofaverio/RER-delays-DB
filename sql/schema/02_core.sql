CREATE TABLE core.dim_time (
    poll_at_utc   TIMESTAMPTZ PRIMARY KEY,
    poll_at_local TIMESTAMPTZ NOT NULL
);

CREATE TABLE core.dim_line (
    line_code TEXT PRIMARY KEY
);

-- Physical station — one row per distinct parent_station in stations.csv.
-- station_id is the numeric part of the IDFM parent_station URI (e.g. IDFM:62890 → 62890).
-- zone_id and station_code are nullable because they are absent for a subset of quays in the source.
CREATE TABLE core.dim_station (
    station_id    INTEGER          PRIMARY KEY,
    stop_name     TEXT             NOT NULL,
    stop_lat      DOUBLE PRECISION NOT NULL,
    stop_lon      DOUBLE PRECISION NOT NULL,
    zone_id       TEXT,
    station_code  TEXT
);

-- Monomodal stop — one level below the physical station.
-- monomodal_code is the integer key shared across all quays of this modal group.
CREATE TABLE core.dim_monomodal_stop (
    monomodal_code    INTEGER PRIMARY KEY,
    monomodal_stop_id TEXT    NOT NULL,  -- e.g. "monomodalStopPlace:47888"
    station_id        INTEGER NOT NULL REFERENCES core.dim_station(station_id)
);

-- Stop as it appears in the delay data — may be a quay (Q), a monomodal stop area (SP),
-- a boarding point (BP), or an unrecognised URI type.
-- stop_type and quay_code are populated by 04_load_stops.sql after stations.csv is loaded.
-- monomodal_code is NULL for:
--   • Q-type stop_ids not present in stations.csv (390 of 570 in day-1 sample)
--   • all BP-type stop_ids (39 in day-1 sample)
--   • any stop_id with an unrecognised URI pattern
-- This is expected and is not a data error.
CREATE TABLE core.dim_stop (
    stop_id         TEXT    PRIMARY KEY,
    stop_type       TEXT    CHECK (stop_type IN ('Q', 'SP', 'BP')),
    quay_code       INTEGER,
    monomodal_code  INTEGER REFERENCES core.dim_monomodal_stop(monomodal_code)
);

CREATE TABLE core.fact_delay_events (
    poll_at_utc      TIMESTAMPTZ      NOT NULL REFERENCES core.dim_time(poll_at_utc),
    stop_id          TEXT             NOT NULL REFERENCES core.dim_stop(stop_id),
    line_code        TEXT             NOT NULL REFERENCES core.dim_line(line_code),
    mean_delay_s     DOUBLE PRECISION NOT NULL,
    mean_lateness_s  DOUBLE PRECISION NOT NULL,
    n                INTEGER          NOT NULL CHECK (n >= 0),
    n_neg            INTEGER          NOT NULL CHECK (n_neg >= 0),
    n_pos            INTEGER          NOT NULL CHECK (n_pos >= 0),
    PRIMARY KEY (poll_at_utc, stop_id, line_code),
    CHECK (n_neg + n_pos <= n),
    CHECK (mean_lateness_s >= 0)
);
