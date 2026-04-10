from pathlib import Path
import json
import pandas as pd

RAW_DIR = Path("data/raw")
OUT_JSON = Path("docs/profile_summary.json")

KEY_COLS = ["poll_at_utc", "stop_id", "line_code"]


def profile_file(path: Path) -> dict:
    df = pd.read_csv(path)

    utc_to_local = (
        df.groupby("poll_at_utc")["poll_at_local"]
        .nunique()
        .reset_index(name="n_local_values")
    )

    profile = {
        "file": path.name,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "null_counts": {k: int(v) for k, v in df.isna().sum().to_dict().items()},
        "duplicate_candidate_key_rows": int(df.duplicated(KEY_COLS).sum()),
        "utc_to_local_violations": int((utc_to_local["n_local_values"] > 1).sum()),
        "unique_stop_ids": int(df["stop_id"].nunique()),
        "unique_line_codes": int(df["line_code"].nunique()),
        "line_codes": sorted(df["line_code"].dropna().unique().tolist()),
        "min_poll_at_utc": str(df["poll_at_utc"].min()),
        "max_poll_at_utc": str(df["poll_at_utc"].max()),
        "mean_delay_min": float(df["mean_delay_s"].min()),
        "mean_delay_max": float(df["mean_delay_s"].max()),
        "mean_lateness_min": float(df["mean_lateness_s"].min()),
        "mean_lateness_max": float(df["mean_lateness_s"].max()),
        "n_min": int(df["n"].min()),
        "n_max": int(df["n"].max()),
    }
    return profile


def main() -> None:
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit("No CSV files found in data/raw")

    profiles = [profile_file(path) for path in files]

    schema_sets = {tuple(p["columns"]) for p in profiles}
    overall = {
        "file_count": len(profiles),
        "schema_consistent": len(schema_sets) == 1,
        "schemas_found": [list(s) for s in schema_sets],
        "profiles": profiles,
    }

    OUT_JSON.write_text(json.dumps(overall, indent=2), encoding="utf-8")

    print(f"Profiled {len(files)} files")
    print(f"Schema consistent: {overall['schema_consistent']}")
    print(f"Output written to {OUT_JSON}")


if __name__ == "__main__":
    main()