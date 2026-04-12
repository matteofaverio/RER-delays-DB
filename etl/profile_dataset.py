from pathlib import Path
import json
import numpy as np
import pandas as pd

RAW_DIR = Path("data/raw")
OUT_JSON = Path("docs/profile_summary.json")

KEY_COLS = ["poll_at_utc", "stop_id", "line_code"]


def _is_all_integer(series: pd.Series) -> bool:
    """True when every non-null value has no fractional part."""
    s = series.dropna()
    if s.empty:
        return True
    return bool((s % 1 == 0).all())


def profile_file(path: Path) -> dict:
    df = pd.read_csv(path)

    utc_to_local = (
        df.groupby("poll_at_utc")["poll_at_local"]
        .nunique()
        .reset_index(name="n_local_values")
    )

    # Check: mean_lateness_s == max(mean_delay_s, 0)
    mask = df["mean_delay_s"].notna() & df["mean_lateness_s"].notna()
    if mask.any():
        expected = df.loc[mask, "mean_delay_s"].clip(lower=0)
        lateness_eq_max = bool(
            np.isclose(df.loc[mask, "mean_lateness_s"], expected).all()
        )
    else:
        lateness_eq_max = True  # no data to contradict

    # Check: n_neg + n_pos <= n
    mask_n = df["n_neg"].notna() & df["n_pos"].notna() & df["n"].notna()
    if mask_n.any():
        n_constraint_holds = bool(
            (df.loc[mask_n, "n_neg"] + df.loc[mask_n, "n_pos"] <= df.loc[mask_n, "n"]).all()
        )
    else:
        n_constraint_holds = True

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
        "mean_delay_s_is_integer": _is_all_integer(df["mean_delay_s"]),
        "mean_lateness_min": float(df["mean_lateness_s"].min()),
        "mean_lateness_max": float(df["mean_lateness_s"].max()),
        "mean_lateness_s_is_integer": _is_all_integer(df["mean_lateness_s"]),
        "mean_lateness_s_equals_max_delay_0": lateness_eq_max,
        "n_min": int(df["n"].min()),
        "n_max": int(df["n"].max()),
        "n_neg_plus_n_pos_lte_n": n_constraint_holds,
    }
    return profile


def main() -> None:
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit("No CSV files found in data/raw")

    profiles = [profile_file(path) for path in files]

    # --- cross-file checks require the full corpus ---
    print(f"Loading {len(files)} files for cross-file checks…")
    combined = pd.concat(
        [pd.read_csv(f) for f in files], ignore_index=True
    )

    # 5. Global duplicates on (poll_at_utc, stop_id, line_code)
    global_dup_count = int(combined.duplicated(KEY_COLS).sum())

    # 6. Distinct line_code values across all files
    global_line_codes = sorted(combined["line_code"].dropna().unique().tolist())

    # 7. Global min/max poll_at_utc
    global_min_poll = str(combined["poll_at_utc"].min())
    global_max_poll = str(combined["poll_at_utc"].max())

    # --- aggregate per-file boolean checks ---
    schema_sets = {tuple(p["columns"]) for p in profiles}
    overall = {
        "file_count": len(profiles),
        "schema_consistent": len(schema_sets) == 1,
        "schemas_found": [list(s) for s in schema_sets],
        # global checks
        "global_duplicate_key_rows": global_dup_count,
        "global_line_codes": global_line_codes,
        "global_min_poll_at_utc": global_min_poll,
        "global_max_poll_at_utc": global_max_poll,
        # aggregate booleans — True only if ALL files pass
        "all_mean_delay_s_is_integer": all(p["mean_delay_s_is_integer"] for p in profiles),
        "all_mean_lateness_s_is_integer": all(p["mean_lateness_s_is_integer"] for p in profiles),
        "all_mean_lateness_s_equals_max_delay_0": all(
            p["mean_lateness_s_equals_max_delay_0"] for p in profiles
        ),
        "all_n_neg_plus_n_pos_lte_n": all(p["n_neg_plus_n_pos_lte_n"] for p in profiles),
        "profiles": profiles,
    }

    OUT_JSON.write_text(json.dumps(overall, indent=2), encoding="utf-8")

    # --- terminal summary ---
    w = 52
    print("\n" + "=" * w)
    print(f"  Files profiled          : {overall['file_count']}")
    print(f"  Schema consistent       : {overall['schema_consistent']}")
    print(f"  Global duplicate keys   : {global_dup_count}")
    print(f"  mean_delay_s integer    : {overall['all_mean_delay_s_is_integer']}")
    print(f"  mean_lateness_s integer : {overall['all_mean_lateness_s_is_integer']}")
    print(f"  lateness = max(delay,0) : {overall['all_mean_lateness_s_equals_max_delay_0']}")
    print(f"  n_neg+n_pos <= n        : {overall['all_n_neg_plus_n_pos_lte_n']}")
    print(f"  poll_at_utc range       : {global_min_poll}  →  {global_max_poll}")
    print(f"  line codes ({len(global_line_codes):>3})        : {', '.join(global_line_codes)}")
    print("=" * w)
    print(f"  Output → {OUT_JSON}")
    print("=" * w)


if __name__ == "__main__":
    main()
