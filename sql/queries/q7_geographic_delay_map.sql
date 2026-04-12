-- Q7: Geographic delay map — station coordinates with average delays
--
-- Purpose:
--   Feed an interactive map (e.g. Leaflet / Mapbox in the dashboard).
--   Each station becomes a marker; colour or size encodes the delay severity.
--   The user picks a line and date range; the map shows which geographic
--   areas of the network are most affected.
--
-- SQL features demonstrated:
--   - Four-table JOIN: fact → dim_stop → dim_monomodal_stop → dim_station
--   - Spatial attributes (stop_lat, stop_lon) from the station dimension
--   - HAVING clause to exclude stations with too few observations
--   - Parameterised on line_code and date range for dashboard filtering
--
-- Grain: one row per station (for the selected line and period).
--
-- Expected result: 30–60 rows per line (number of stations varies by line).
--
-- Dashboard widget: interactive map with coloured markers

SELECT
    ds.stop_name,
    ds.stop_lat,
    ds.stop_lon,
    ds.zone_id,
    f.line_code,
    round(avg(f.mean_delay_s)::numeric,    2)      AS avg_delay_s,
    round(avg(f.mean_lateness_s)::numeric, 2)      AS avg_lateness_s,
    sum(f.n)                                       AS total_trains,
    count(*)                                       AS n_polls
FROM core.fact_delay_events    f
JOIN core.dim_stop              s  ON s.stop_id        = f.stop_id
JOIN core.dim_monomodal_stop   ms ON ms.monomodal_code = s.monomodal_code
JOIN core.dim_station          ds ON ds.station_id     = ms.station_id
WHERE s.stop_type  = 'SP'
  AND f.line_code  = :line_code                    -- e.g. 'RER A'
  AND f.poll_at_utc >= :date_from                  -- e.g. '2026-03-01'
  AND f.poll_at_utc <  :date_to                    -- e.g. '2026-04-01'
GROUP BY ds.stop_name, ds.stop_lat, ds.stop_lon, ds.zone_id, f.line_code
HAVING count(*) >= 10                              -- exclude noisy stations
ORDER BY avg_lateness_s DESC;
