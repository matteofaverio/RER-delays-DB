"""
ETL pipeline for RER delays DB.

Execution order
---------------
1. (Optional --reset) Run 00_reset_empty_schemas.sql   — drops and recreates schemas
2. Run 01_staging.sql                                   — creates staging tables
3. Run 02_core.sql                                      — creates core dimension/fact tables
4. COPY each data/raw/*.csv   → staging.delay_events
5. COPY data/stations.csv     → staging.stops
6. Run 03_load_core_from_staging.sql  — populate core dims + fact from staging delay data
7. Run 04_load_stops.sql              — populate dim_station, dim_monomodal_stop;
                                        enrich dim_stop with type/quay/monomodal columns

Usage
-----
    python etl/load.py [--reset]

    --reset   Drop and recreate all schemas before loading (destructive).
              Omit for incremental loads (ON CONFLICT DO NOTHING guards are in place).

Connection
----------
Reads PGDATABASE, PGUSER, PGPASSWORD (optional), PGHOST, PGPORT from environment
or from a .env file in the project root.
"""

import argparse
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
SQL_DIR      = PROJECT_ROOT / "sql" / "schema"
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
STATIONS_CSV = PROJECT_ROOT / "data" / "stations.csv"


def _sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect() -> psycopg.Connection:
    load_dotenv(PROJECT_ROOT / ".env")
    params = {
        "dbname":   os.environ["PGDATABASE"],
        "user":     os.environ["PGUSER"],
        "host":     os.environ.get("PGHOST", "localhost"),
        "port":     int(os.environ.get("PGPORT", 5432)),
    }
    if pw := os.environ.get("PGPASSWORD"):
        params["password"] = pw
    return psycopg.connect(**params, autocommit=False)


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------

def _table_exists(cur: psycopg.Cursor, schema: str, table: str) -> bool:
    return cur.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s)",
        (schema, table),
    ).fetchone()[0]


def setup_schema(conn: psycopg.Connection, *, reset: bool) -> None:
    with conn.cursor() as cur:
        if reset:
            print("  Resetting schemas…")
            cur.execute(_sql("00_reset_empty_schemas.sql"))

        if not _table_exists(cur, "staging", "delay_events"):
            print("  Creating staging tables…")
            cur.execute(_sql("01_staging.sql"))
        else:
            print("  Staging tables already exist — skipping.")

        if not _table_exists(cur, "core", "fact_delay_events"):
            print("  Creating core tables…")
            cur.execute(_sql("02_core.sql"))
        else:
            print("  Core tables already exist — skipping.")

    conn.commit()


# ---------------------------------------------------------------------------
# Staging loads
# ---------------------------------------------------------------------------

def load_delay_csvs(conn: psycopg.Connection) -> int:
    """COPY all data/raw/*.csv into staging.delay_events. Returns rows inserted."""
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit("No CSV files found in data/raw")

    total = 0
    with conn.cursor() as cur:
        for path in files:
            # Read header to confirm column order matches staging table expectation.
            with path.open(encoding="utf-8") as fh:
                header = fh.readline().strip().split(",")

            expected = [
                "poll_at_utc", "poll_at_local", "stop_id", "line_code",
                "mean_delay_s", "mean_lateness_s", "n", "n_neg", "n_pos",
            ]
            if header != expected:
                raise ValueError(
                    f"{path.name}: unexpected header {header!r}; expected {expected!r}"
                )

            # source_file is not in the CSV; we COPY the 9 data columns then UPDATE.
            row_count_before = cur.execute(
                "SELECT COUNT(*) FROM staging.delay_events WHERE source_file = %s",
                (path.name,)
            ).fetchone()[0]

            if row_count_before == 0:
                with path.open(encoding="utf-8") as fh:
                    with cur.copy(
                        "COPY staging.delay_events "
                        "(poll_at_utc, poll_at_local, stop_id, line_code, "
                        " mean_delay_s, mean_lateness_s, n, n_neg, n_pos) "
                        "FROM STDIN (FORMAT CSV, HEADER TRUE)"
                    ) as copy:
                        copy.write(fh.read())

                # Tag the freshly inserted rows (source_file was NULL for them)
                cur.execute(
                    "UPDATE staging.delay_events SET source_file = %s "
                    "WHERE source_file IS NULL",
                    (path.name,)
                )
                inserted = cur.execute(
                    "SELECT COUNT(*) FROM staging.delay_events WHERE source_file = %s",
                    (path.name,)
                ).fetchone()[0]
                print(f"  Loaded {path.name}: {inserted:>7,} rows")
                total += inserted
            else:
                print(f"  Skipped {path.name} (already loaded: {row_count_before:,} rows)")

    conn.commit()
    return total


def load_stations(conn: psycopg.Connection) -> int:
    """COPY data/stations.csv into staging.stops. Returns rows inserted."""
    if not STATIONS_CSV.exists():
        raise SystemExit(f"stations.csv not found at {STATIONS_CSV}")

    with conn.cursor() as cur:
        existing = cur.execute("SELECT COUNT(*) FROM staging.stops").fetchone()[0]
        if existing > 0:
            print(f"  Skipped stations.csv (staging.stops already has {existing:,} rows)")
            conn.rollback()
            return 0

        with STATIONS_CSV.open(encoding="utf-8") as fh:
            with cur.copy(
                "COPY staging.stops "
                "(quay_code, monomodal_stop_id, stop_id_idfm, monomodal_code, "
                " stop_name, parent_station, stop_lat, stop_lon, zone_id, "
                " location_type, station_code) "
                "FROM STDIN (FORMAT CSV, HEADER TRUE)"
            ) as copy:
                copy.write(fh.read())

        n = cur.execute("SELECT COUNT(*) FROM staging.stops").fetchone()[0]
    conn.commit()
    print(f"  Loaded stations.csv: {n:>7,} rows")
    return n


# ---------------------------------------------------------------------------
# Core transforms
# ---------------------------------------------------------------------------

def transform_delay(conn: psycopg.Connection) -> None:
    print("  Transforming staging delay data → core…")
    with conn.cursor() as cur:
        cur.execute(_sql("03_load_core_from_staging.sql"))
        times = cur.execute("SELECT COUNT(*) FROM core.dim_time").fetchone()[0]
        lines = cur.execute("SELECT COUNT(*) FROM core.dim_line").fetchone()[0]
        stops = cur.execute("SELECT COUNT(*) FROM core.dim_stop").fetchone()[0]
        facts = cur.execute("SELECT COUNT(*) FROM core.fact_delay_events").fetchone()[0]
    conn.commit()
    print(f"    dim_time: {times:,}  dim_line: {lines}  dim_stop: {stops:,}  fact: {facts:,}")


def transform_stops(conn: psycopg.Connection) -> None:
    print("  Populating station dimensions…")
    with conn.cursor() as cur:
        cur.execute(_sql("04_load_stops.sql"))
        stations       = cur.execute("SELECT COUNT(*) FROM core.dim_station").fetchone()[0]
        monomodal      = cur.execute("SELECT COUNT(*) FROM core.dim_monomodal_stop").fetchone()[0]
        enriched       = cur.execute(
            "SELECT COUNT(*) FROM core.dim_stop WHERE stop_type IS NOT NULL"
        ).fetchone()[0]
        unresolved     = cur.execute(
            "SELECT COUNT(*) FROM core.dim_stop WHERE monomodal_code IS NULL"
        ).fetchone()[0]
    conn.commit()
    print(f"    dim_station: {stations:,}  dim_monomodal_stop: {monomodal:,}")
    print(f"    dim_stop enriched: {enriched:,}  unresolved (monomodal_code NULL): {unresolved:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Load RER delay data into PostgreSQL.")
    parser.add_argument(
        "--reset", action="store_true",
        help="Drop and recreate all schemas before loading (destructive)."
    )
    args = parser.parse_args()

    print("Connecting…")
    conn = connect()

    try:
        print("\n[1/5] Schema setup")
        setup_schema(conn, reset=args.reset)

        print("\n[2/5] Load delay CSVs → staging.delay_events")
        total_rows = load_delay_csvs(conn)
        print(f"  Total new rows: {total_rows:,}")

        print("\n[3/5] Load stations.csv → staging.stops")
        load_stations(conn)

        print("\n[4/5] Transform delay data → core")
        transform_delay(conn)

        print("\n[5/5] Transform stops → core")
        transform_stops(conn)

        print("\nDone.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
