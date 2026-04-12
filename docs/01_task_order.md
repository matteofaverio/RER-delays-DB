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
9. ~~**Write analytical queries**~~ — Q1–Q9 written and verified in `sql/queries/` ✓
   - Q1: weekly average delay by line
   - Q2: worst stations by mean lateness (SP grain)
   - Q3: delay by hour-of-day and day-of-week
   - Q4: station ranking with percentile context
   - Q5: station delay timeline (point lookup, dashboard drill-down)
   - Q6: line comparison over date range (index range scan)
   - Q7: geographic delay map (lat/lon for interactive map)
   - Q8: peak vs off-peak comparison (CASE WHEN conditional aggregation)
   - Q9: delay distribution histogram (width_bucket)
10. ~~**EXPLAIN ANALYZE**~~ — all 9 queries profiled; notes in `docs/03_query_notes.md` ✓
11. ~~**Forced-plan comparison**~~ — natural vs forced plan, 289× cost difference documented ✓
12. **Report** — sections assigned across team (see `docs/04_team_briefing.md`)
13. **Dashboard** — Streamlit, 5 pages, assigned to colleague
14. **Presentation** — after report is drafted

## Resolved questions

- SP-type stop_ids used for station-level aggregation (Q2, Q4, Q5, Q7) to avoid double-counting
- `dim_time` kept as physical table — used by Q3 and Q8 for local-time extraction
- `n`, `n_neg`, `n_pos` semantics remain inferred (not confirmed from IDFM docs)
