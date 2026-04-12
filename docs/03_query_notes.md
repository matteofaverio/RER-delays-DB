# Analytical query notes

All queries run against `core.*` tables on 11,815,213 fact rows.
Session setting `SET work_mem = '64MB'` applied for all runs to eliminate disk spills.

---

## Q1 — Weekly average delay by line

**File:** `sql/queries/q1_weekly_delay_by_line.sql`

**Purpose:** Track how mean delay and lateness evolve over time for each RER line.
Reveals seasonal patterns, degradation periods, and incident spikes.

**Grain:** one row per (line, ISO week). All stop types included — the goal is
the network-wide signal, not station detail.

**Expected result:** 110 rows (5 lines × 22 weeks).
`avg_delay_s` can be negative (trains systematically early). `avg_lateness_s >= 0` always.

**Execution time:** 1,384 ms

**EXPLAIN ANALYZE summary:**
- Plan: Parallel Seq Scan → Partial HashAggregate → Gather → Finalize HashAggregate → Sort
- Workers launched: 2
- No index used — correct. This query reads every row; no selective predicate exists.
  Full-table aggregations cannot benefit from B-tree indexes.

**Indexing notes:** No additional index warranted. For a narrower time window
(e.g. a single month), `idx_fact_line_time (line_code, poll_at_utc)` would be used.

---

## Q2 — Worst stations by mean lateness, per line

**File:** `sql/queries/q2_worst_stations_by_lateness.sql`

**Purpose:** Rank all stations on each line by average lateness to identify
chronic delay hotspots. Combines fact data with the station dimension hierarchy.

**Grain:** SP-type stop_ids only (one aggregated area row per station per poll).
Q-type and BP-type rows excluded to avoid double-counting stations that appear
at both quay and area granularity.

**Expected result:** 254 rows (varies by line: ~30–60 stations per line).
Terminus and interchange stations expected to rank highest.

**Sample results (top 3 across all lines):**
| Line | Station | avg_lateness_s |
|---|---|---|
| RER E | Villiers-sur-Marne – Le Plessis-Trévise | 69.42 s |
| RER E | Nogent – Le Perreux | 69.29 s |
| RER E | Le Raincy – Villemomble – Montfermeil | 66.13 s |

**Execution time:** 1,129 ms

**EXPLAIN ANALYZE summary:**
- Plan: Parallel Seq Scan on fact + 3× Hash Join (dim_stop → dim_monomodal_stop → dim_station)
  → Partial HashAggregate → Gather Merge → Finalize GroupAggregate → Incremental Sort
- Hash tables for the three small dimensions (908, 476, 475 rows) fit entirely in memory.
- No disk spill with `work_mem = '64MB'`.
- No index used on fact — joining all SP rows (≈6 M of 11.8 M) is too large for index scan.

**Indexing notes:** A partial index `ON fact_delay_events (stop_id) WHERE stop_id LIKE 'STIF:StopArea:SP:%'`
could help if this query is run frequently with an added line filter. Not added yet —
benefit must be confirmed with EXPLAIN after adding a WHERE clause per line.

---

## Q3 — Average delay by hour-of-day and day-of-week

**File:** `sql/queries/q3_delay_by_hour_and_dow.sql`

**Purpose:** Reveal commuter-hour patterns. Are delays worse at peak hours (7–9h, 17–19h)?
Are weekends structurally different? Uses `poll_at_local` (local Paris time) for
human-meaningful hour interpretation.

**Grain:** all stop types included (network-wide signal per time slot).
Requires a join to `dim_time` to access `poll_at_local`.

**Expected result:** 789 rows (5 lines × up to 7 days × 24 hours; some night hours absent).
Peak commute hours expected to show higher `avg_lateness_s`.

**Execution time:** 2,213 ms

**EXPLAIN ANALYZE summary:**
- Plan: Parallel Seq Scan on fact + Hash Join to dim_time (37,146 rows, fits in 2.3 MB hash)
  → Partial HashAggregate → Gather → Finalize HashAggregate → Sort
- The join to `dim_time` is cheap: the hash table is small and built once per worker.
- The extra cost vs Q1 (~800 ms) comes entirely from the `to_char` and two `EXTRACT`
  expressions evaluated on 11.8 M rows.

**Indexing notes:** No index helps here — every row is read for the aggregation.
If local-time extraction is a recurring bottleneck, computed columns
`hour_local` and `dow` could be added to `fact_delay_events` to avoid the join
and the expression evaluation. Not recommended until query frequency justifies it.

---

## Q4 — Station ranking with percentile context, per line

**File:** `sql/queries/q4_station_ranking_per_line.sql`

**Purpose:** For each line, rank every station by average lateness and show its
percentile within the line. Answers "how bad is bad?": a station at the 95th
percentile is a genuine outlier vs. the rest of the line.

**Grain:** SP-type stop_ids only (same reasoning as Q2).
Uses two window functions (`rank()` and `percent_rank()`) over the per-line CTE result.

**Expected result:** 254 rows (same station set as Q2). `rank = 1` is the worst station.
`pct_rank = 1.00` at top, `0.00` at bottom.

**Execution time:** 1,118 ms

**EXPLAIN ANALYZE summary:**
- Plan: same base as Q2 (Parallel Seq Scan + 3× Hash Join + GroupAggregate) feeding into
  3× Incremental Sort + 2× WindowAgg for `rank()` and `percent_rank()`.
- All window function sorts fit in memory (< 31 kB each — only 254 rows in the window input).
- No disk spill. Overhead vs Q2 is negligible (~11 ms): the window functions are trivial
  at this cardinality.

**Indexing notes:** Same as Q2. No additional index warranted.

---

## Summary table

| Query | Rows returned | Execution time | Plan type |
|---|---|---|---|
| Q1 weekly delay by line | 110 | 1,384 ms | Parallel Seq Scan |
| Q2 worst stations by lateness | 254 | 1,129 ms | Parallel Seq Scan + Hash Join ×3 |
| Q3 delay by hour and day-of-week | 789 | 2,213 ms | Parallel Seq Scan + Hash Join |
| Q4 station ranking with percentile | 254 | 1,118 ms | Parallel Seq Scan + Hash Join ×3 + WindowAgg |

All queries run in 1–2 seconds on 11.8 M rows without any server tuning.
The dominant cost in every case is the sequential scan of `fact_delay_events` (~130,000
8 kB pages = ~1 GB on disk). Parallel workers reduce wall time by ~3×.

The only query that would meaningfully benefit from an additional index is a
**single-line, time-bounded** variant of Q1 or Q2 using `idx_fact_line_time`.
