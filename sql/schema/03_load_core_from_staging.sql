INSERT INTO core.dim_time (poll_at_utc, poll_at_local)
SELECT DISTINCT poll_at_utc, poll_at_local
FROM staging.delay_events
ON CONFLICT (poll_at_utc) DO NOTHING;

INSERT INTO core.dim_line (line_code)
SELECT DISTINCT line_code
FROM staging.delay_events
ON CONFLICT (line_code) DO NOTHING;

INSERT INTO core.dim_stop (stop_id)
SELECT DISTINCT stop_id
FROM staging.delay_events
ON CONFLICT (stop_id) DO NOTHING;

INSERT INTO core.fact_delay_events (
    poll_at_utc,
    stop_id,
    line_code,
    mean_delay_s,
    mean_lateness_s,
    n,
    n_neg,
    n_pos
)
SELECT DISTINCT
    poll_at_utc,
    stop_id,
    line_code,
    mean_delay_s,
    mean_lateness_s,
    n,
    n_neg,
    n_pos
FROM staging.delay_events
ON CONFLICT (poll_at_utc, stop_id, line_code) DO NOTHING;