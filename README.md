# RER Delays DB

A relational database system for analysing train delays on the Paris RER network, built as a university DBMS course project.

We self-collected five months of real-time delay data from the IDFM PRIM API (November 2025 – April 2026), producing 150 daily CSV files and **11,815,213 observations** across the five RER lines (A–E). The project covers the full database engineering lifecycle: schema design, ETL pipeline, query optimisation with `EXPLAIN ANALYZE`, and an interactive Streamlit dashboard.

## Key results

- **Star-snowflake schema** — one fact table and five dimension tables modelling the three-level IDFM stop hierarchy (station → monomodal stop → quay/platform)
- **Idempotent ETL** — `COPY FROM STDIN` bulk-load (10–100× faster than `INSERT`); indexes deferred to post-load to maximise throughput
- **289× query slowdown** reproduced via forced B-tree index on a 100%-selectivity full-table scan, confirming the planner's cost model

## Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 16 |
| ETL & profiling | Python 3.12 — psycopg, pandas |
| Dashboard | Streamlit 1.x |

## Repository structure

```
sql/
  schema/      DDL scripts — staging schema, core schema, indexes (run in order 00–05)
  queries/     Analytical SQL queries Q1–Q9
etl/
  load.py               ETL pipeline: CSV → staging → core
  profile_dataset.py    Automated data profiler (generates summary statistics)
dashboard/
  app.py                Streamlit entry point (Line comparison — Q6)
  db.py                 Database connection helper and query cache
  pages/                Additional dashboard pages (Q5, Q7, Q8)
data/
  sample/     Three representative daily CSV files (Nov 2025, Jan 2026, Mar 2026)
  stations.csv   IDFM stop reference file (stop IDs, names, coordinates, zone)
```

## Database dump

A full PostgreSQL dump of the loaded database (`rer_delays_db.dump.zip`, ~240 MB) is available on request. It can be restored with:

```bash
unzip rer_delays_db.dump.zip
pg_restore -d rer_delays_db rer_delays_db.dump
```

## Setup from scratch

**Prerequisites:** PostgreSQL 16, Python 3.12+, and the full dataset in `data/raw/` (150 daily CSVs).

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Configure database connection
cp .env.example .env
# Edit .env with your PostgreSQL credentials (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)

# 3. Create the database
createdb rer_delays_db

# 4. Run the ETL pipeline  (idempotent — safe to re-run)
python etl/load.py

# 5. Launch the dashboard
streamlit run dashboard/app.py
```

Pass `--reset` to `etl/load.py` to drop all schemas and rebuild from scratch (development use only).

### Running SQL scripts manually

```bash
# Apply schema in order
psql -d rer_delays_db -f sql/schema/00_reset_empty_schemas.sql
psql -d rer_delays_db -f sql/schema/01_staging.sql
psql -d rer_delays_db -f sql/schema/02_core.sql
psql -d rer_delays_db -f sql/schema/03_load_core_from_staging.sql
psql -d rer_delays_db -f sql/schema/04_load_stops.sql
psql -d rer_delays_db -f sql/schema/05_indexes.sql
```

