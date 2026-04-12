-- Q3: Average delay by hour-of-day and day-of-week
--
-- Purpose:
--   Reveal commuter-hour patterns: are delays worse during peak hours (8h, 18h)?
--   Are weekends structurally different from weekdays?
--   Uses local time (poll_at_local) for human-readable hour/day interpretation.
--
-- Grain: one row per (line, day-of-week, hour).
-- All stop types included — we want network-wide signal, not station detail.
--
-- Expected result:
--   5 lines × 7 days × 24 hours = 840 rows (some hours may be absent if no polls).
--   Peak hours (7–9, 17–19) expected to show higher avg_lateness_s.
--   Weekend rows (dow 0=Sunday, 6=Saturday) expected to show lower or different pattern.

SELECT
    f.line_code,
    extract(isodow FROM t.poll_at_local)::int   AS dow,       -- 1=Mon … 7=Sun
    to_char(t.poll_at_local, 'Dy')              AS dow_name,
    extract(hour   FROM t.poll_at_local)::int   AS hour_local,
    round(avg(f.mean_delay_s)::numeric,    2)   AS avg_delay_s,
    round(avg(f.mean_lateness_s)::numeric, 2)   AS avg_lateness_s,
    count(*)                                    AS n_observations
FROM core.fact_delay_events f
JOIN core.dim_time          t ON t.poll_at_utc = f.poll_at_utc
GROUP BY f.line_code, dow, dow_name, hour_local
ORDER BY f.line_code, dow, hour_local;
