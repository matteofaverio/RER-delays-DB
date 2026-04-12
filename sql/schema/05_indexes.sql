-- Indexes for core.fact_delay_events (11,815,213 rows)
-- Added after bulk load to avoid per-row maintenance overhead during COPY.
--
-- Query patterns driving these choices:
--   Q1  time-series by line      → WHERE / GROUP BY line_code, date_trunc(poll_at_utc)
--   Q2  worst stops by lateness  → WHERE line_code = X  ORDER BY mean_lateness_s
--   Q3  time-of-day patterns     → GROUP BY date_part('hour', poll_at_utc)
--   Q4  station rollup           → JOIN dim_stop ON stop_id, GROUP BY monomodal_code

-- Covers Q1 and Q2: filtering/grouping on line_code with poll_at_utc range scan.
-- Column order: line_code first (equality predicate), then poll_at_utc (range/sort).
CREATE INDEX idx_fact_line_time
    ON core.fact_delay_events (line_code, poll_at_utc);

-- Covers Q3 and global time-range filters independent of line.
CREATE INDEX idx_fact_time
    ON core.fact_delay_events (poll_at_utc);

-- Covers per-stop lookups and joins from dim_stop in Q2 / Q4.
CREATE INDEX idx_fact_stop
    ON core.fact_delay_events (stop_id);

-- Covers station-rollup joins: fact → dim_stop → dim_monomodal_stop.
-- Partial index: only rows that are actually resolved save index space.
CREATE INDEX idx_stop_monomodal
    ON core.dim_stop (monomodal_code)
    WHERE monomodal_code IS NOT NULL;
