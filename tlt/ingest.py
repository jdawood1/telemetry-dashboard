from __future__ import annotations
from pathlib import Path
import pandas as pd


def ingest_csv(input_path: str | Path, out_path: str | Path) -> Path:
    """
    Ingest a CSV of telemetry events and write normalized Parquet.

    Required columns:
      - timestamp (ISO8601; parsed to tz-aware UTC)
      - user_id    (string)
      - event      (string)
      - feature_id (string)

    Optional columns:
      - latency_ms (numeric; coerced if present)
    """
    input_path, out_path = Path(input_path), Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Read with stable dtypes; avoid "object" surprises in groupbys
    df = pd.read_csv(
        input_path,
        dtype={
            "user_id": "string",
            "event": "string",
            "feature_id": "string",
        },
        # Keep literal "NA" strings as data (not NaN); we’ll validate nulls explicitly
        keep_default_na=False,
    )

    # ---- Schema & null validation ----
    required = {"timestamp", "user_id", "event", "feature_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Normalize timestamp to tz-aware UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if df["timestamp"].isna().any():
        n = int(df["timestamp"].isna().sum())
        raise ValueError(f"{n} timestamps could not be parsed.")

    # Coerce latency if present (non-fatal; rows keep NaN where invalid)
    if "latency_ms" in df.columns:
        df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")

    # Enforce non-null for key identifiers
    for col in ("user_id", "event", "feature_id"):
        if df[col].isna().any() or (df[col].astype("string").str.len() == 0).any():
            raise ValueError(f"Column '{col}' contains null/empty values.")

    # Deterministic order helps diffs and downstream expectations
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Prefer compression if available; fall back gracefully
    try:
        df.to_parquet(out_path, index=False, compression="zstd")
    except Exception:
        try:
            df.to_parquet(out_path, index=False, compression="snappy")
        except Exception:
            df.to_parquet(out_path, index=False)

    return out_path
