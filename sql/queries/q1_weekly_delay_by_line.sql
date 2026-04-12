-- Q1: Weekly average delay and lateness by line
--
-- Purpose:
--   Track how mean delay and mean lateness evolve over time for each RER line.
--   Useful for spotting seasonal patterns, degradation periods, or incident spikes.
--
-- Grain: one row per (line, ISO week).
-- No stop filter needed — the fact table already has one row per (poll, stop, line),
-- so averaging over all stops gives the network-wide signal for that line.
--
-- Expected result:
--   110 rows (5 lines × 22 weeks in the corpus).
--   mean_delay_s can be negative (trains systematically early on some lines/weeks).
--   mean_lateness_s is always >= 0 by construction.

SELECT
    line_code,
    date_trunc('week', poll_at_utc)::date          AS week_start,
    round(avg(mean_delay_s)::numeric,    2)        AS avg_delay_s,
    round(avg(mean_lateness_s)::numeric, 2)        AS avg_lateness_s,
    round(stddev(mean_delay_s)::numeric, 2)        AS stddev_delay_s,
    count(*)                                       AS n_observations
FROM core.fact_delay_events
GROUP BY line_code, week_start
ORDER BY line_code, week_start;
