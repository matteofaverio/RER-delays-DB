CREATE TABLE staging.delay_events (
    poll_at_utc       TIMESTAMPTZ NOT NULL,
    poll_at_local     TIMESTAMPTZ NOT NULL,
    stop_id           TEXT NOT NULL,
    line_code         TEXT NOT NULL,
    mean_delay_s      DOUBLE PRECISION NOT NULL,
    mean_lateness_s   DOUBLE PRECISION NOT NULL,
    n                 INTEGER NOT NULL,
    n_neg             INTEGER NOT NULL,
    n_pos             INTEGER NOT NULL,
    source_file       TEXT,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw load of data/stations.csv — one row per quay (platform/track face).
-- The file encodes a three-level hierarchy in denormalised form:
--   quay_code → monomodal_code → parent_station (physical station).
-- Columns mirror the CSV exactly; no transformation is applied here.
-- zone_id and station_code are nullable in the source.
CREATE TABLE staging.stops (
    quay_code         INTEGER  NOT NULL,  -- numeric quay identifier (= numeric part of stop_id_idfm)
    monomodal_stop_id TEXT     NOT NULL,  -- e.g. "monomodalStopPlace:47888"
    stop_id_idfm      TEXT     NOT NULL,  -- e.g. "IDFM:47888"
    monomodal_code    INTEGER  NOT NULL,  -- groups quays that share a modal stop
    stop_name         TEXT     NOT NULL,
    parent_station    TEXT     NOT NULL,  -- e.g. "IDFM:62890" — the physical station
    stop_lat          DOUBLE PRECISION NOT NULL,
    stop_lon          DOUBLE PRECISION NOT NULL,
    zone_id           TEXT,              -- nullable: absent for ~10 % of quays
    location_type     SMALLINT NOT NULL, -- always 0 in this dataset (stop/platform)
    station_code      TEXT,              -- nullable: absent for ~43 % of quays
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);