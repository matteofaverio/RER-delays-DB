-- Q5: Delay timeline for a single station
--
-- Purpose:
--   Given a station name, show its daily average delay and lateness over the
--   full observation period.  This is the primary dashboard drill-down:
--   the user clicks a station on the map or picks it from a dropdown,
--   and sees a time-series chart.
--
-- SQL features demonstrated:
--   - Parameterised query (:station_name) for dashboard binding
--   - Three-table JOIN through the stop hierarchy (dim_stop → dim_monomodal_stop → dim_station)
--   - date_trunc to aggregate to daily grain
--   - Index benefit: idx_fact_stop turns this into a sub-millisecond lookup
--     on the fact table (Bitmap Index Scan), unlike the full-table scans of Q1–Q4.
--
-- Grain: one row per day, SP-type stop_ids only.
--
-- Expected result:
--   Up to ~150 rows (one per calendar day in the corpus).
--   Useful for spotting station-specific incidents or seasonal effects.
--
-- Dashboard widget: line chart (x = date, y = avg_delay_s / avg_lateness_s)

SELECT
    date_trunc('day', f.poll_at_utc)::date          AS day,
    round(avg(f.mean_delay_s)::numeric,    2)       AS avg_delay_s,
    round(avg(f.mean_lateness_s)::numeric, 2)       AS avg_lateness_s,
    round(stddev(f.mean_delay_s)::numeric, 2)       AS stddev_delay_s,
    sum(f.n)                                        AS total_trains,
    count(*)                                        AS n_polls
FROM core.fact_delay_events    f
JOIN core.dim_stop              s  ON s.stop_id        = f.stop_id
JOIN core.dim_monomodal_stop   ms ON ms.monomodal_code = s.monomodal_code
JOIN core.dim_station          ds ON ds.station_id     = ms.station_id
WHERE s.stop_type  = 'SP'
  AND ds.stop_name = :station_name          -- e.g. 'Chatelet-Les-Halles'
GROUP BY day
ORDER BY day;
