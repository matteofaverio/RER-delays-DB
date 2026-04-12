-- Q8: Peak vs off-peak delay comparison
--
-- Purpose:
--   Compare delay metrics between rush-hour and off-peak periods.
--   Answers the dashboard question: "Are delays concentrated during commute hours?"
--   Peak hours defined as 07:00–09:59 and 17:00–19:59 local time (Paris).
--
-- SQL features demonstrated:
--   - CASE WHEN for conditional aggregation (single pass, no self-join)
--   - FILTER clause (PostgreSQL extension) as an alternative conditional agg
--   - JOIN to dim_time for local-time extraction
--   - Comparison across two dimensions: line × peak/offpeak
--
-- Grain: one row per (line, period).
--
-- Expected result: 10 rows (5 lines × 2 periods).
--   Peak hours expected to show higher avg_lateness due to crowding and
--   knock-on effects from tighter headways.
--
-- Dashboard widget: grouped bar chart (groups = lines, bars = peak/offpeak)

SELECT
    f.line_code,
    CASE
        WHEN extract(hour FROM t.poll_at_local) BETWEEN 7 AND 9
          OR extract(hour FROM t.poll_at_local) BETWEEN 17 AND 19
        THEN 'peak'
        ELSE 'offpeak'
    END                                              AS period,
    round(avg(f.mean_delay_s)::numeric,    2)       AS avg_delay_s,
    round(avg(f.mean_lateness_s)::numeric, 2)       AS avg_lateness_s,
    round(stddev(f.mean_delay_s)::numeric, 2)       AS stddev_delay_s,
    sum(f.n)                                        AS total_trains,
    count(*)                                        AS n_observations
FROM core.fact_delay_events f
JOIN core.dim_time          t ON t.poll_at_utc = f.poll_at_utc
GROUP BY f.line_code, period
ORDER BY f.line_code, period;
