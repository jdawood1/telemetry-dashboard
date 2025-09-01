from pathlib import Path
import pandas as pd


def ingest_csv(input_path: str | Path, out_path: str | Path) -> Path:
    input_path, out_path = Path(input_path), Path(out_path)

    # auto-create parent dir (so "data/" exists)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # read input CSV
    df = pd.read_csv(input_path)

    # basic sanity: required cols
    required = {"timestamp", "user_id", "event", "feature_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # save as parquet
    df.to_parquet(out_path, index=False)

    return out_path
