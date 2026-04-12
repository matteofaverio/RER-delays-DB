-- Q4: Station ranking per line with percentile context
--
-- Purpose:
--   For each line, rank every station by average lateness and show where it falls
--   in the distribution (percentile). Combines Q2's ranking with context that makes
--   "how bad is bad?" answerable: a station at the 95th percentile is a real outlier.
--
-- Grain: SP-type stop_ids only (same reasoning as Q2).
--
-- Expected result:
--   One row per (line, station). rank = 1 is the worst station on that line.
--   pct_rank = 1.00 at the top, 0.00 at the bottom.
--   Useful for identifying whether delay is concentrated at a few hotspots
--   or spread uniformly across the line.

WITH station_avg AS (
    SELECT
        f.line_code,
        ds.stop_name,
        ds.station_code,
        ds.zone_id,
        round(avg(f.mean_lateness_s)::numeric, 2)    AS avg_lateness_s,
        round(avg(f.mean_delay_s)::numeric,    2)    AS avg_delay_s,
        count(*)                                     AS n_polls
    FROM core.fact_delay_events    f
    JOIN core.dim_stop              s  ON s.stop_id        = f.stop_id
    JOIN core.dim_monomodal_stop   ms ON ms.monomodal_code = s.monomodal_code
    JOIN core.dim_station          ds ON ds.station_id     = ms.station_id
    WHERE s.stop_type = 'SP'
    GROUP BY f.line_code, ds.stop_name, ds.station_code, ds.zone_id
)
SELECT
    line_code,
    stop_name,
    station_code,
    zone_id,
    avg_lateness_s,
    avg_delay_s,
    n_polls,
    rank()       OVER (PARTITION BY line_code ORDER BY avg_lateness_s DESC) AS rank,
    round(
        percent_rank() OVER (PARTITION BY line_code ORDER BY avg_lateness_s)::numeric,
        2
    )                                                                        AS pct_rank
FROM station_avg
ORDER BY line_code, rank;
