from __future__ import annotations
from pathlib import Path
import pandas as pd


def transform_parquet(in_path: str | Path, out_path: str | Path) -> Path:
    """
    Read raw events parquet, compute daily aggregates, and write an aggregated parquet.

    Produces:
      - events per (date, feature_id)
      - dau per date (distinct users/day)
      - if 'latency_ms' exists: p50/p95 latency per (date, feature_id)
    """
    in_path, out_path = Path(in_path), Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_path)

    # ensure timestamp -> date
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    # events per (date, feature)
    events = df.groupby(["date", "feature_id"]).size().reset_index(name="events")

    # DAU per day
    dau = df.groupby("date")["user_id"].nunique().reset_index(name="dau")

    # optional latency metrics if present
    if "latency_ms" in df.columns:
        lat = (
            df.groupby(["date", "feature_id"])["latency_ms"]
            .agg(p50=lambda s: s.quantile(0.5), p95=lambda s: s.quantile(0.95))
            .reset_index()
        )
        agg = events.merge(lat, on=["date", "feature_id"], how="left")
    else:
        agg = events.copy()
        agg["p50"] = pd.NA
        agg["p95"] = pd.NA

    # join DAU (broadcast per date)
    agg = agg.merge(dau, on="date", how="left")

    agg.to_parquet(out_path, index=False)
    return out_path
