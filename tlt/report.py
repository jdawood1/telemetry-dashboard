from pathlib import Path
import pandas as pd
import matplotlib

matplotlib.use("Agg")  # CI/headless
import matplotlib.pyplot as plt


def make_reports(in_path: str | Path, out_dir: str | Path) -> Path:
    in_path, out_dir = Path(in_path), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_path)

    # ---- Determine schema: raw vs aggregated ----
    is_agg = {"date", "feature_id", "events"}.issubset(df.columns)

    # ---- Feature usage chart (works for raw or agg) ----
    if is_agg:
        usage = df.groupby("feature_id")["events"].sum().sort_values(ascending=False)
    else:
        # raw: count events per feature
        usage = df.groupby("feature_id").size().sort_values(ascending=False)

    plt.figure()
    usage.plot(kind="bar")
    plt.title("Feature Usage (Total Events)")
    plt.xlabel("feature_id")
    plt.ylabel("events")
    plt.tight_layout()
    plt.savefig(out_dir / "feature_usage.png")
    plt.close()

    # ---- Text metrics ----
    metrics_path = out_dir / "metrics.txt"
    with open(metrics_path, "w") as f:
        f.write("=== Telemetry Summary ===\n")

        if is_agg:
            total_events = int(df["events"].sum())
            n_days = df["date"].nunique()
            n_features = df["feature_id"].nunique()
            f.write(f"Aggregated days: {n_days}\n")
            f.write(f"Total events: {total_events}\n")
            f.write(f"Features: {n_features}\n")

            # If DAU column exists, report stats
            if "dau" in df.columns:
                # overall unique users can't be derived exactly from agg;
                # report mean/max DAU instead
                dau_by_day = df.drop_duplicates("date")[["date", "dau"]].set_index(
                    "date"
                )["dau"]
                f.write(f"Mean DAU: {float(dau_by_day.mean()):.1f}\n")
                f.write(f"Max DAU: {int(dau_by_day.max())}\n")

            # Optional latency if present
            if {"p50", "p95"}.issubset(df.columns):
                f.write(
                    f"Median latency p50 (overall mean): {pd.to_numeric(df['p50'], errors='coerce').mean():.1f} ms\n"
                )
                f.write(
                    f"Tail latency p95 (overall mean): {pd.to_numeric(df['p95'], errors='coerce').mean():.1f} ms\n"
                )

        else:
            # raw schema
            total_events = len(df)
            n_users = df["user_id"].nunique()
            n_features = df["feature_id"].nunique()
            n_days = pd.to_datetime(df["timestamp"]).dt.date.nunique()
            f.write(f"Days: {n_days}\n")
            f.write(f"Events: {total_events}\n")
            f.write(f"Unique users: {n_users}\n")
            f.write(f"Features: {n_features}\n")

    return out_dir
