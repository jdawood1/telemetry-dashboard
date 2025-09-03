from __future__ import annotations
from pathlib import Path
import pandas as pd


def _compute_mau(events: pd.DataFrame, window_days: int = 30) -> pd.DataFrame:
    """
    Exact MAU per day: count of unique users active in the trailing `window_days` (inclusive).
    Requires 'date' as DatetimeIndex-like column (floored to day).
    """
    active = events[["date", "user_id"]].drop_duplicates().assign(present=1)
    pivot = active.pivot_table(
        index="date", columns="user_id", values="present", aggfunc="max", fill_value=0
    )
    # time-based rolling window (e.g., '30D')
    rolled = pivot.rolling(f"{window_days}D", min_periods=1).sum()
    mau = (rolled > 0).sum(axis=1).rename(f"mau_{window_days}d")
    return mau.reset_index()  # columns: ['date', f'mau_{window_days}d']


def transform_parquet(
        in_path: str | Path,
        out_path: str | Path,
        mau_window: int = 30,
) -> Path:
    """
    Read raw events parquet, compute daily aggregates, and write an aggregated parquet.

    Produces one row per (date, feature_id):
      - events
      - p50 / p95 latency (if latency_ms present)
      - dau per day (duplicated across features for convenience)
      - mau_{mau_window}d per day (duplicated across features)
    """
    in_path, out_path = Path(in_path), Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_path)

    # Ensure timestamp and 'date' (floor to day, keep as datetime64 for parquet + rolling ops)
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if ts.isna().any():
        raise ValueError("Some timestamps could not be parsed.")
    df = df.copy()
    df["date"] = ts.dt.floor("D")

    # events per (date, feature)
    events = df.groupby(["date", "feature_id"]).size().reset_index(name="events")

    # optional latency metrics if present
    if "latency_ms" in df.columns:
        lat = (
            df.groupby(["date", "feature_id"])["latency_ms"]
            .agg(p50=lambda s: float(s.quantile(0.5)), p95=lambda s: float(s.quantile(0.95)))
            .reset_index()
        )
        agg = events.merge(lat, on=["date", "feature_id"], how="left")
    else:
        agg = events.assign(p50=pd.NA, p95=pd.NA)

    # DAU per day (distinct users)
    dau = df.groupby("date")["user_id"].nunique().reset_index(name="dau")
    agg = agg.merge(dau, on="date", how="left")

    # MAU (rolling exact)
    mau = _compute_mau(df, window_days=mau_window)
    agg = agg.merge(mau, on="date", how="left")

    agg.to_parquet(out_path, index=False)
    return out_path
