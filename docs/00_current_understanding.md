# Current understanding

## Directly observed across all 150 CSV files (2025-11-12 → 2026-04-10)

- Columns (9, consistent across all files):
  `poll_at_utc`, `poll_at_local`, `stop_id`, `line_code`,
  `mean_delay_s`, `mean_lateness_s`, `n`, `n_neg`, `n_pos`
- Natural key `(poll_at_utc, stop_id, line_code)` is globally unique (0 duplicates)
- `poll_at_utc → poll_at_local` is deterministic within each file (0 violations)
- `mean_delay_s` and `mean_lateness_s` are floating-point (not integers)
- `mean_lateness_s` is NOT `max(mean_delay_s, 0)` — the two metrics are computed independently
- `mean_lateness_s >= 0` always holds
- `n_neg + n_pos <= n` always holds
- No nulls in any column in any file
- 5 line codes: RER A, B, C, D, E
- ~767 distinct `stop_id` values per day

## stop_id is not a station identifier

Three URI formats appear in the delay data:

| Pattern | Day-1 count | Meaning |
|---|---|---|
| `STIF:StopPoint:Q:<n>:` | 570 | Quay (individual platform) |
| `STIF:StopArea:SP:<n>:` | 158 | Monomodal stop area (groups quays) |
| `STIF:StopPoint:BP:<n>:` | 39 | Boarding point (no match in stations.csv) |

The real stop hierarchy (from `data/stations.csv`, 2533 rows):

```
dim_station (475)          ← physical station (e.g. "Gare de Lyon")
  └─ dim_monomodal_stop (476)
       └─ dim_stop / quay (2533)
```

- 158/158 SP ids resolve to a `monomodal_code` in stations.csv
- 180/570 Q ids resolve to a `quay_code` in stations.csv; 390 are unresolvable (expected)
- 39 BP ids have no match in stations.csv (expected)
- Unresolved stop_ids get `monomodal_code = NULL` in `dim_stop` — this is not a data error

**Warning:** delay data has mixed granularity. Some `stop_id`s are quay-level (Q), others
are area-level (SP). Aggregating to station level must filter to one granularity or risk
double-counting.

## What is still inference (not proven)

- `n` = number of trains sampled during the poll interval (name-based only)
- `n_neg` = early trains, `n_pos` = late trains (plausible from names, undocumented)
- Whether `mean_delay_s` and `mean_lateness_s` use different base definitions
- Whether the 390 unmatched Q-ids are from lines outside the station CSV's scope
- Whether `stop_id` cardinality is stable across all 150 days (checked on day 1 only)

## Current project decisions

- PostgreSQL is the source of truth
- SQL is explicit and versioned under `sql/schema/`
- Raw CSVs are immutable
- Data flows: raw CSV → `staging` → `core` normalized tables
- Metrics stored as `DOUBLE PRECISION` (confirmed correct)
- Indexes added after bulk loading, not before

## Pending decisions

- Whether `dim_time` remains a physical dimension or local time is computed in queries
- Final index strategy (pending EXPLAIN ANALYZE on real queries)
- Whether to add a `stop_granularity` column to `fact_delay_events` to make
  the Q/SP mixed-granularity issue queryable without joining back to `dim_stop`
