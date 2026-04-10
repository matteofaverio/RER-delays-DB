# RER-delays-DB

Database Management Systems project on IDFM RER delay data.

## Main goals
- design a normalized PostgreSQL schema from flat daily CSV files
- build a reproducible ETL pipeline in Python
- write and optimize analytical SQL queries
- later expose selected results in a lightweight dashboard

## Stack
- PostgreSQL
- Python

## Structure
- `data/raw/`: original CSV files, kept local
- `data/sample/`: small sample files if needed
- `sql/schema/`: DDL scripts
- `sql/queries/`: analytical queries
- `sql/views/`: reusable SQL views
- `etl/`: ingestion and transformation scripts
- `docs/`: design notes and schema reasoning
