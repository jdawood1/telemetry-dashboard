# tests/test_flow.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

from tlt.ingest import ingest_csv
from tlt.transform import transform_parquet
from tlt.report import make_reports

# Resolve the sample file relative to the repo root, not CWD
REPO = Path(__file__).resolve().parents[1]
SAMPLE = REPO / "sample" / "events.csv"


def test_flow(tmp_path: Path) -> None:
    raw = tmp_path / "events.parquet"
    agg = tmp_path / "agg.parquet"
    out = tmp_path / "reports"

    # Ingest
    p_raw = ingest_csv(SAMPLE, raw)
    assert raw.exists()
    assert p_raw == raw

    # Transform
    p_agg = transform_parquet(raw, agg)
    assert agg.exists()
    assert p_agg == agg

    # Basic schema sanity
    df_agg = pd.read_parquet(agg)
    assert {"date", "feature_id", "events"}.issubset(df_agg.columns)
    assert "dau" in df_agg.columns  # produced per-day

    # Report
    p_out = make_reports(agg, out)
    assert p_out == out
    assert (out / "feature_usage.png").exists()

    metrics = out / "metrics.txt"
    assert metrics.exists()
    txt = metrics.read_text(encoding="utf-8")
    assert "Telemetry Summary" in txt
