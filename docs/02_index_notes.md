# Index notes

## Context

Indexes were added after bulk load of 11,815,213 rows into `core.fact_delay_events`.
Adding them before loading would have incurred per-row maintenance cost on every COPY.
All timings below were measured on a local macOS PostgreSQL instance (no tuning beyond defaults).

---

## Indexes created (`sql/schema/05_indexes.sql`)

| Index | Table | Columns | Type | Size |
|---|---|---|---|---|
| `idx_fact_line_time` | `fact_delay_events` | `(line_code, poll_at_utc)` | B-tree | 82 MB |
| `idx_fact_time` | `fact_delay_events` | `(poll_at_utc)` | B-tree | 81 MB |
| `idx_fact_stop` | `fact_delay_events` | `(stop_id)` | B-tree | 81 MB |
| `idx_stop_monomodal` | `dim_stop` | `(monomodal_code) WHERE NOT NULL` | Partial B-tree | 16 kB |

The primary key `(poll_at_utc, stop_id, line_code)` already existed (863 MB).

---

## EXPLAIN ANALYZE results

### Q1 — weekly average delay by line (full corpus, all lines)

```sql
SELECT line_code,
       date_trunc('week', poll_at_utc) AS week,
       avg(mean_delay_s)               AS avg_delay_s
FROM core.fact_delay_events
GROUP BY line_code, week
ORDER BY line_code, week;
```

| | Execution time | Plan |
|---|---|---|
| Before indexes | 1,588 ms | Parallel Seq Scan |
| After indexes | 1,079 ms | Parallel Seq Scan |

**Observation:** planner correctly keeps the sequential scan. This query reads every row
regardless of indexes — no selective predicate. The modest improvement (32 %) is from
warm buffer cache, not index use. For full-table aggregations, indexes cannot help.

---

### Q1-filtered — weekly average delay, one line

```sql
SELECT date_trunc('week', poll_at_utc) AS week,
       avg(mean_delay_s)               AS avg_delay_s
FROM core.fact_delay_events
WHERE line_code = 'RER A'
GROUP BY week
ORDER BY week;
```

| Execution time | Plan |
|---|---|
| 796 ms | Parallel Seq Scan (filter on line_code) |

**Observation:** planner still prefers a seq scan. RER A accounts for ~30 % of rows
(~3.6 M rows). At that selectivity, reading the whole table with parallel workers is
faster than a random-access index scan. `idx_fact_line_time` would be beneficial for
much narrower time ranges (e.g. a single day or week) or if `work_mem` were raised to
allow index-only plans.

---

### Q2 — worst stops by mean lateness, one line, SP grain

```sql
SELECT f.stop_id, ds.stop_name,
       avg(f.mean_lateness_s) AS avg_lateness_s,
       count(*)               AS n_polls
FROM core.fact_delay_events f
JOIN core.dim_stop           s  ON s.stop_id        = f.stop_id
JOIN core.dim_monomodal_stop ms ON ms.monomodal_code = s.monomodal_code
JOIN core.dim_station        ds ON ds.station_id     = ms.station_id
WHERE f.line_code = 'RER B'
  AND s.stop_type = 'SP'
GROUP BY f.stop_id, ds.stop_name
ORDER BY avg_lateness_s DESC
LIMIT 10;
```

| Execution time | Plan |
|---|---|
| 1,462 ms | Parallel Seq Scan + Hash Join + external merge sort (disk: ~23 MB/worker) |

**Observation:** RER B is ~20 % of rows (~2.4 M), still too large for an index scan.
The external merge sort to disk indicates `work_mem` is a bottleneck. Raising
`work_mem` (e.g. `SET work_mem = '64MB'`) for analytical sessions would eliminate
the disk spill. No schema change needed.

---

### Point lookup — one stop, one line

```sql
SELECT poll_at_utc, mean_delay_s, mean_lateness_s, n
FROM core.fact_delay_events
WHERE stop_id   = 'STIF:StopPoint:Q:412851:'
  AND line_code = 'RER B'
ORDER BY poll_at_utc;
```

| Execution time | Plan |
|---|---|
| **0.9 ms** | Bitmap Index Scan on `idx_fact_stop` → Bitmap Heap Scan |

**Observation:** `idx_fact_stop` is used immediately for single-stop lookups.
Sub-millisecond. This is the access pattern a dashboard would use.

---

## Summary and recommendations

1. **Bulk aggregations** (all lines, all time) will always seq-scan — acceptable for
   offline analysis; no further indexing needed for that pattern.

2. **Per-line aggregations** currently seq-scan because each line is ~20–30 % of the
   table. `idx_fact_line_time` will help significantly once queries add a time range
   (e.g. `WHERE poll_at_utc >= '2026-01-01'`). Worth testing with a 30-day window.

3. **Single-stop lookups** are fast (< 1 ms) thanks to `idx_fact_stop`.

4. **External sort to disk** in Q2 can be eliminated by setting `work_mem = '64MB'`
   at the session level for analytical queries. No DDL change needed.

5. **No additional indexes** are warranted before query profiling with real workloads.
   Unnecessary indexes cost space (each B-tree on `fact_delay_events` ≈ 81 MB) and
   slow down future incremental loads.
