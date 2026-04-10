# Current understanding

## Directly observed in the sample CSV
- Columns:
  - poll_at_utc
  - poll_at_local
  - stop_id
  - line_code
  - mean_delay_s
  - mean_lateness_s
  - n
  - n_neg
  - n_pos
- Composite event key candidate: (poll_at_utc, stop_id, line_code)
- poll_at_utc determines poll_at_local in the sample
- stop_id and line_code are independent entities

## Current project decisions
- PostgreSQL is the source of truth
- SQL remains explicit and versioned under sql/
- Raw CSV files are immutable
- Data is loaded into staging first, then transformed into core normalized tables
- Indexes are added after initial bulk loading, not before

## Decisions still pending
- Final numeric type for delay metrics
- Whether dim_time remains a physical dimension or local time is computed in queries
- Final index strategy after real query profiling