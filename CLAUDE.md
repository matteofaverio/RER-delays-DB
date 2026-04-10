# Project Context

This is a university DBMS project on IDFM RER delay analysis.

Primary goals:
1. Design a normalized PostgreSQL schema from denormalized CSV data.
2. Build a reproducible ETL pipeline.
3. Write analytical SQL and inspect performance with EXPLAIN ANALYZE.
4. Only after the database is stable, expose selected queries in a dashboard.

# Working Rules

- Prioritize explicit SQL over ORM-heavy abstractions.
- Never invent columns or assumptions: inspect the CSV files first.
- Keep raw data immutable.
- Load raw CSV data into staging tables before transforming into normalized tables.
- Store schema changes in versioned SQL files under `sql/schema/`.
- For each important analytical query, also record:
  - purpose
  - expected result
  - EXPLAIN ANALYZE notes
  - indexing notes
- Do not commit full raw datasets or secrets.
- Prefer small, reviewable changes.