-- Q9: Delay distribution — histogram buckets per line
--
-- Purpose:
--   Show the distribution shape of delays on each line: is it a tight cluster
--   around 0, or a long tail of severe delays?  Feeds a histogram or density
--   chart on the dashboard.
--
-- SQL features demonstrated:
--   - width_bucket() for histogram binning (a built-in PostgreSQL function)
--   - generate_series() to label bucket boundaries
--   - CTE to precompute per-line ranges
--   - Demonstrates why full-table seq scan is unavoidable for distribution queries
--
-- Bucket design:
--   20 fixed buckets from -120 s to +300 s (range covers >99% of observations).
--   Bucket width = (300 - (-120)) / 20 = 21 s each.
--   Values outside the range land in bucket 0 (< -120) or 21 (> 300).
--
-- Grain: one row per (line, bucket).
--
-- Expected result: ~100 rows (5 lines × 20 buckets + edge buckets).
--   Most observations expected in the 0–60 s range.
--
-- Dashboard widget: histogram / bar chart per line

WITH buckets AS (
    SELECT
        line_code,
        width_bucket(mean_delay_s, -120, 300, 20)   AS bucket,
        count(*)                                    AS n_observations
    FROM core.fact_delay_events
    GROUP BY line_code, bucket
)
SELECT
    b.line_code,
    b.bucket,
    -- Human-readable bucket boundaries
    CASE
        WHEN b.bucket = 0  THEN '< -120'
        WHEN b.bucket = 21 THEN '> 300'
        ELSE round((-120 + (b.bucket - 1) * 21.0)::numeric, 0)::text
          || ' to '
          || round((-120 + b.bucket * 21.0)::numeric, 0)::text
    END                                             AS bucket_range_s,
    b.n_observations,
    round(
        100.0 * b.n_observations
        / sum(b.n_observations) OVER (PARTITION BY b.line_code),
        2
    )                                               AS pct_of_line
FROM buckets b
ORDER BY b.line_code, b.bucket;
