# Task order

## Completed

1. Profile all CSV files — full 150-file corpus, cross-file checks ✓
2. Freeze the raw data contract — schema, key, types, constraints verified ✓
3. Create staging schema — `staging.delay_events`, `staging.stops` ✓
4. Create normalized core schema — `dim_time`, `dim_line`, `dim_stop`,
   `dim_monomodal_stop`, `dim_station`, `fact_delay_events` ✓
5. Write ETL pipeline — `etl/load.py` orchestrates all schema and load steps ✓
6. Write transform SQL — `03_load_core_from_staging.sql`, `04_load_stops.sql` ✓

## Up next

7. ~~**Run the pipeline**~~ — 11,815,213 fact rows loaded; all constraints satisfied ✓
8. ~~**Add indexes**~~ — 4 indexes created; EXPLAIN ANALYZE recorded in `docs/02_index_notes.md` ✓
9. ~~**Write analytical queries**~~ — Q1–Q4 written and verified in `sql/queries/` ✓
   - Q1: weekly average delay by line
   - Q2: worst stations by mean lateness (SP grain)
   - Q3: delay by hour-of-day and day-of-week
   - Q4: station ranking with percentile context
10. ~~**EXPLAIN ANALYZE**~~ — all four queries profiled; notes in `docs/03_query_notes.md` ✓
11. **Dashboard** — only after schema and queries are stable

## Open questions before step 9

- Confirm semantics of `n`, `n_neg`, `n_pos` from IDFM documentation
- Decide whether to filter to one stop granularity (Q or SP) for station aggregates,
  or add a `stop_granularity` column to `fact_delay_events`
- Decide whether `dim_time` stays as a physical table or gets dropped in favour of
  extracting time attributes in queries
