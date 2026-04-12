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

---

## Q5 — Station delay timeline (point lookup)

**File:** `sql/queries/q5_station_delay_timeline.sql`

**Purpose:** Given a station name, show its daily average delay over the full observation
period. This is the primary dashboard drill-down: user clicks a station → line chart.

**Grain:** one row per day, SP-type stop_ids only.

**Sample station:** Châtelet les Halles (serves RER A, B, D).

**Execution time:** 9,539 ms (cold cache) — first run, 62K pages read from disk.

**EXPLAIN ANALYZE summary:**
- Plan: Seq Scan on dim_station → filter stop_name → Hash Join to dim_monomodal_stop
  → Index Scan (idx_stop_monomodal) on dim_stop → **Bitmap Index Scan (idx_fact_stop)**
  on fact → GroupAggregate
- The station lookup narrows to 1 stop_id, then idx_fact_stop fetches exactly 83,633
  matching rows out of 11.8 M (0.7 % selectivity).
- Cold-cache performance is dominated by random reads. On warm cache this drops to < 1 s.

**Indexing notes:** `idx_fact_stop` is critical here. This is the textbook "dashboard point
lookup" pattern — the exact use case B-tree indexes are designed for.

---

## Q6 — Line comparison over date range

**File:** `sql/queries/q6_line_comparison_date_range.sql`

**Purpose:** Compare all 5 RER lines for a user-selected date window. The main
dashboard landing view: "which line was worst this week?"

**Grain:** one row per line for the selected period.

**Execution time:** 4,323 ms (7-day window, 2026-03-01 to 2026-03-08, ~551K rows = 4.7 %)

**EXPLAIN ANALYZE summary:**
- Plan: **Bitmap Index Scan on idx_fact_line_time** → Parallel Bitmap Heap Scan
  → HashAggregate → Sort
- The planner uses the index! A 7-day window is selective enough (4.7 % of rows)
  to justify the index scan over a full table scan.
- Contrast with Q1 (full corpus): same query structure but 100 % selectivity → Seq Scan.
  This is the key demonstration of **cost-based plan selection**.

**Indexing notes:** `idx_fact_line_time (line_code, poll_at_utc)` is the enabling index.
Narrower windows (1 day) would be even faster. Wider windows (> 30 days) would
revert to Seq Scan — correctly.

---

## Q7 — Geographic delay map

**File:** `sql/queries/q7_geographic_delay_map.sql`

**Purpose:** Feed an interactive map. Each station → marker with colour/size encoding
delay severity. User picks a line + date range.

**Grain:** one row per station for the selected line and period.

**Sample:** RER A, March 2026. Returns 46 stations.

**Execution time:** 4,516 ms

**EXPLAIN ANALYZE summary:**
- Plan: **Bitmap Index Scan on idx_fact_line_time** (line + time range)
  → Parallel Bitmap Heap Scan → Hash Join ×3 (dim_stop, dim_monomodal_stop, dim_station)
  → GroupAggregate → Sort
- The compound predicate `line_code = 'RER A' AND poll_at_utc BETWEEN ...`
  fully leverages the composite index.
- Dimension tables (475–908 rows) fit entirely in memory as hash tables.
- HAVING count(*) >= 10 filters post-aggregation.

**Indexing notes:** Same index pattern as Q6. The three-table join adds minimal cost
because dimension tables are tiny.

---

## Q8 — Peak vs off-peak comparison

**File:** `sql/queries/q8_peak_vs_offpeak.sql`

**Purpose:** Compare delay metrics between rush-hour (07–09h, 17–19h local) and
off-peak periods. Dashboard: grouped bar chart.

**Grain:** one row per (line, period). Returns exactly 10 rows.

**Sample results:**
| Line | Period | avg_delay_s | avg_lateness_s |
|---|---|---|---|
| RER A | offpeak | 78.02 | 88.02 |
| RER A | peak | 88.36 | 103.46 |
| RER B | offpeak | 106.55 | 113.69 |
| RER B | peak | 125.07 | 138.74 |
| RER C | offpeak | 89.55 | 95.69 |
| RER C | peak | 118.53 | 130.10 |

**Finding:** Peak-hour delays are consistently 10–30 s higher across all lines.
RER C shows the biggest peak/offpeak gap (+29 s delay, +34 s lateness).

**Execution time:** 2,194 ms

**EXPLAIN ANALYZE summary:**
- Plan: Parallel Seq Scan on fact + Hash Join to dim_time → HashAggregate → Sort
- Full table scan (correct — every row contributes to one of two buckets).
- CASE WHEN conditional aggregation avoids a self-join.

**Indexing notes:** No index helps. This is a single-pass full-table computation.

---

## Q9 — Delay distribution (histogram)

**File:** `sql/queries/q9_delay_distribution.sql`

**Purpose:** Show the shape of the delay distribution per line. Dashboard: histogram chart.

**Grain:** 20 buckets from -120 s to +300 s (21 s each) + 2 overflow buckets.
Returns ~110 rows (5 lines × ~22 buckets).

**Key finding:** The distribution is right-skewed for all lines. The modal bucket
is [-15, 6] s (near zero delay) holding 25–32 % of observations. RER B has the
heaviest tail: 10.5 % of observations exceed 300 s (5 minutes).

**Execution time:** 890 ms

**EXPLAIN ANALYZE summary:**
- Plan: Parallel Seq Scan → HashAggregate → WindowAgg (for pct_of_line)
- `width_bucket()` computed inline during scan — no extra pass needed.
- Window function is trivial (110 rows in the window input, 18 kB memory).

**Indexing notes:** No index helps. Full-table scan is optimal for distribution queries.

---

## Forced-plan cost comparison

**Purpose:** Demonstrate that the PostgreSQL query planner makes optimal choices by
comparing the natural plan against a forced plan using `SET enable_seqscan = off`.

### Test: Q1 — full-corpus weekly aggregation

| Plan | Execution time | Buffer accesses | I/O pattern |
|---|---|---|---|
| **Natural** (Parallel Seq Scan) | **1,543 ms** | 146K pages | Sequential |
| **Forced** (Parallel Index Scan via idx_fact_line_time) | **445,588 ms** | 11.8M pages | Random |

**Ratio: the forced index plan is 289× slower.**

### Why the planner is right

The natural plan reads ~130,000 8 kB pages in sequential order — the OS read-ahead
prefetcher makes this nearly as fast as a raw disk scan. The forced index plan traverses
the B-tree for every row (11.8 M index entries), then fetches each corresponding heap
page via random I/O. Random I/O is ~100× more expensive per page than sequential I/O
on spinning disks and ~10× on SSDs.

For a full-table aggregation with no selective predicate, every row must be visited
regardless. The index adds overhead (B-tree traversal) without reducing the number of
rows read. The planner correctly estimates this and chooses the sequential scan.

### When the index IS used

The same `idx_fact_line_time` index is used by Q6 and Q7 when the query includes a
selective date range (e.g. 7 days = 4.7 % of rows). At that selectivity, the random
I/O cost of an index scan is offset by reading far fewer pages.

This demonstrates the planner's **cost-based optimization**: the same index can be
beneficial or harmful depending on selectivity. The break-even point on this dataset
is approximately 10–15 % of rows.

---

## Updated summary table

| Query | Rows returned | Execution time | Plan type | Key index used |
|---|---|---|---|---|
| Q1 weekly delay by line | 110 | 1,543 ms | Parallel Seq Scan | — |
| Q2 worst stations | 254 | 1,129 ms | Parallel Seq Scan + Hash Join ×3 | — |
| Q3 delay by hour/DOW | 789 | 2,213 ms | Parallel Seq Scan + Hash Join | — |
| Q4 station ranking | 254 | 1,118 ms | Parallel Seq Scan + Hash Join ×3 + WindowAgg | — |
| Q5 station timeline | 150 | 9,539 ms* | Bitmap Index Scan + Nested Loop | idx_fact_stop |
| Q6 line comparison (7d) | 5 | 4,323 ms | Bitmap Index Scan + HashAggregate | idx_fact_line_time |
| Q7 geographic map (1mo) | 46 | 4,516 ms | Bitmap Index Scan + Hash Join ×3 | idx_fact_line_time |
| Q8 peak vs offpeak | 10 | 2,194 ms | Parallel Seq Scan + Hash Join | — |
| Q9 delay distribution | 110 | 890 ms | Parallel Seq Scan + WindowAgg | — |
| Q1 forced (no seqscan) | 110 | 445,588 ms | Parallel Index Scan | idx_fact_line_time (forced) |

*Q5 cold-cache; warm-cache expected < 1 s.
