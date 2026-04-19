# RER Delays DB

A relational database system for analysing train delays on the Paris RER network, built as a university DBMS course project.

We self-collected five months of real-time delay data from the IDFM PRIM API (November 2025 – April 2026), producing 150 daily CSV files and 11,815,213 observations across the five RER lines (A–E). The project covers the full database engineering lifecycle: schema design, ETL, query optimisation with `EXPLAIN ANALYZE`, and an interactive Streamlit dashboard.

## Key findings

- **Star-snowflake schema** with one fact table and five dimension tables, modelling the three-level IDFM stop hierarchy (station → monomodal stop → quay/platform)
- **Idempotent ETL** using `COPY FROM STDIN` (10–100× faster than INSERT); indexes deferred to post-load
- **289× slowdown** in the forced-plan experiment: forcing a B-tree index on a 100%-selectivity full-table scan confirms the planner's cost model

## Stack

- PostgreSQL 16
- Python 3.12 (psycopg, pandas, Streamlit)

## Repository structure

```
sql/
  schema/      DDL scripts (staging + core schemas, indexes)
  queries/     Analytical SQL queries (Q1–Q9)
etl/
  load.py      ETL pipeline: CSV → staging → core
  profile_dataset.py  Automated data profiler
dashboard/
  app.py       Streamlit entry point (Page 1 — Line comparison)
  db.py        Database connection and caching
  pages/       Additional dashboard pages (Q5, Q7, Q8)
data/
  raw/         Daily CSV files (kept local, not committed)
  stations.csv IDFM stop reference file
docs/
  report.tex   Full project report (LaTeX)
  slides.tex   Beamer presentation slides (LaTeX)
```

## Setup

**Prerequisites:** PostgreSQL 16, Python 3.12+

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Configure database connection
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 3. Run the ETL pipeline
python etl/load.py

# 4. Launch the dashboard
streamlit run dashboard/app.py
```

The ETL pipeline is idempotent — re-running it on an already-loaded database is safe.  
Pass `--reset` to drop all schemas and rebuild from scratch (development only).

