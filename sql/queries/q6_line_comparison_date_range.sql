-- Q6: Line comparison over a user-selected date range
--
-- Purpose:
--   Compare all 5 RER lines side-by-side for a date window chosen by the user.
--   This is the main dashboard landing view: a bar chart showing which line
--   performed best/worst over the selected period.
--
-- SQL features demonstrated:
--   - Range predicate on poll_at_utc → benefits from idx_fact_line_time
--     when the window is narrow (< 30 days), unlike full-corpus Q1.
--   - No JOIN needed — pure fact-table query (fastest possible).
--   - Demonstrates how index selectivity changes with range width:
--     a 7-day window reads ~5% of the table → planner uses Index Scan.
--     the full corpus reads 100% → planner falls back to Seq Scan.
--
-- Grain: one row per line.
--
-- Expected result: exactly 5 rows (one per RER line).
--
-- Dashboard widget: horizontal bar chart (x = avg_lateness_s, y = line_code)

SELECT
    line_code,
    round(avg(mean_delay_s)::numeric,    2)        AS avg_delay_s,
    round(avg(mean_lateness_s)::numeric, 2)        AS avg_lateness_s,
    round(stddev(mean_delay_s)::numeric, 2)        AS stddev_delay_s,
    round(max(mean_delay_s)::numeric,    2)        AS max_delay_s,
    sum(n)                                         AS total_trains,
    count(*)                                       AS n_observations
FROM core.fact_delay_events
WHERE poll_at_utc >= :date_from                    -- e.g. '2026-03-01'
  AND poll_at_utc <  :date_to                      -- e.g. '2026-03-08'
GROUP BY line_code
ORDER BY avg_lateness_s DESC;
