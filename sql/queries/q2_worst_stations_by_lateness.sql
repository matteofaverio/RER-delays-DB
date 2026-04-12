-- Q2: Worst stations by mean lateness, per line
--
-- Purpose:
--   Rank stations by their average lateness to identify chronic delay hotspots.
--   Useful for comparing performance across stations on the same line.
--
-- Grain: SP-type stop_ids only (one row per station per poll, already aggregated
--   by the data source). Q-type and BP-type rows are excluded to avoid
--   double-counting stations that appear at both quay and area granularity.
--
-- Expected result:
--   One row per (line, station). Each line has between 30–100 stations.
--   avg_lateness_s > 0 always. Terminus stations tend to rank highest.
--
-- Parameter: set :line to the desired line code, e.g. 'RER B'

SELECT
    f.line_code,
    ds.stop_name,
    ds.station_code,
    round(avg(f.mean_lateness_s)::numeric, 2)   AS avg_lateness_s,
    round(avg(f.mean_delay_s)::numeric,    2)   AS avg_delay_s,
    round(stddev(f.mean_lateness_s)::numeric, 2) AS stddev_lateness_s,
    count(*)                                    AS n_polls,
    sum(f.n)                                    AS total_trains_sampled
FROM core.fact_delay_events    f
JOIN core.dim_stop              s  ON s.stop_id        = f.stop_id
JOIN core.dim_monomodal_stop   ms ON ms.monomodal_code = s.monomodal_code
JOIN core.dim_station          ds ON ds.station_id     = ms.station_id
WHERE s.stop_type = 'SP'
GROUP BY f.line_code, ds.stop_name, ds.station_code
ORDER BY f.line_code, avg_lateness_s DESC;
