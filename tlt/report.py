from __future__ import annotations
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # CI/headless
import matplotlib.pyplot as plt


def _plot_feature_usage(series: pd.Series, out_path: Path) -> None:
    plt.figure()
    series.plot(kind="bar")
    plt.title("Feature Usage (Total Events)")
    plt.xlabel("feature_id")
    plt.ylabel("events")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def _plot_latency_box_by_feature(events: pd.DataFrame, out_path: Path) -> None:
    """Boxplot of latency_ms grouped by feature_id (from raw events)."""
    if not {"feature_id", "latency_ms"}.issubset(events.columns):
        return
    data = []
    labels = []
    for feat, df_f in events.groupby("feature_id"):
        s = pd.to_numeric(df_f["latency_ms"], errors="coerce").dropna()
        if not s.empty:
            data.append(s.values)
            labels.append(str(feat))
    if not data:
        return
    plt.figure()
    plt.boxplot(data, labels=labels, showfliers=False)
    plt.title("Latency by Feature (boxplot)")
    plt.ylabel("Latency (ms)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def make_reports(in_path: str | Path, out_dir: str | Path, events_path: str | Path | None = None) -> Path:
    in_path, out_dir = Path(in_path), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_path)
    events_df = None
    if events_path:
        try:
            events_df = pd.read_parquet(events_path)
        except Exception:
            events_df = None  # Optional

    # ---- Determine schema: raw vs aggregated ----
    is_agg = {"date", "feature_id", "events"}.issubset(df.columns)

    # ---- Feature usage chart ----
    if is_agg:
        usage = df.groupby("feature_id")["events"].sum().sort_values(ascending=False)
    else:
        usage = df.groupby("feature_id").size().sort_values(ascending=False)
    _plot_feature_usage(usage, out_dir / "feature_usage.png")

    # ---- Optional latency-by-feature chart from raw events ----
    if events_df is not None:
        _plot_latency_box_by_feature(events_df, out_dir / "latency_by_feature.png")

    # ---- Text metrics ----
    metrics_path = out_dir / "metrics.txt"
    with open(metrics_path, "w", encoding="utf-8") as f:
        f.write("=== Telemetry Summary ===\n")

        if is_agg:
            total_events = int(df["events"].sum())
            n_days = pd.to_datetime(df["date"]).dt.floor("D").nunique()
            n_features = df["feature_id"].nunique()
            f.write(f"Aggregated days: {n_days}\n")
            f.write(f"Total events: {total_events}\n")
            f.write(f"Features: {n_features}\n")

            if "dau" in df.columns:
                dau_by_day = df.groupby("date")["dau"].max()
                f.write(f"Mean DAU: {float(dau_by_day.mean()):.1f}\n")
                f.write(f"Max DAU: {int(dau_by_day.max())}\n")

            mau_col = next((c for c in df.columns if str(c).startswith("mau_")), None)
            if mau_col:
                latest_mau = int(df.sort_values("date").groupby("date")[mau_col].max().iloc[-1])
                f.write(f"{mau_col.upper()} (most recent day): {latest_mau}\n")

            # If p50/p95 exist in aggregated rows, report overall means of those stats
            if {"p50", "p95"}.issubset(df.columns):
                mean_p50 = pd.to_numeric(df["p50"], errors="coerce").mean()
                mean_p95 = pd.to_numeric(df["p95"], errors="coerce").mean()
                if pd.notna(mean_p50):
                    f.write(f"Median latency p50 (overall mean): {mean_p50:.1f} ms\n")
                if pd.notna(mean_p95):
                    f.write(f"Tail latency p95 (overall mean): {mean_p95:.1f} ms\n")
        else:
            # raw schema
            total_events = len(df)
            n_users = df["user_id"].nunique()
            n_features = df["feature_id"].nunique()
            n_days = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.floor("D").nunique()
            f.write(f"Days: {n_days}\n")
            f.write(f"Events: {total_events}\n")
            f.write(f"Unique users: {n_users}\n")
            f.write(f"Features: {n_features}\n")

    return out_dir

