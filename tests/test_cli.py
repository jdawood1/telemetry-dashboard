from __future__ import annotations

from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
SAMPLE = REPO / "sample" / "events.csv"


def _py() -> str:
    return sys.executable


def test_cli_smoke(tmp_path: Path) -> None:
    data = tmp_path / "data"
    reports = tmp_path / "reports"
    events_parquet = data / "events.parquet"
    agg_parquet = data / "agg.parquet"

    # ingest
    subprocess.check_call([
        _py(), "-m", "tlt.cli", "ingest",
        "--input", str(SAMPLE),
        "--out", str(events_parquet),
    ])
    assert events_parquet.exists()

    # transform
    subprocess.check_call([
        _py(), "-m", "tlt.cli", "transform",
        "--in", str(events_parquet),
        "--out", str(agg_parquet),
    ])
    assert agg_parquet.exists()

    # report
    subprocess.check_call([
        _py(), "-m", "tlt.cli", "report",
        "--in", str(agg_parquet),
        "--out", str(reports),
    ])
    assert (reports / "feature_usage.png").exists()
    assert (reports / "metrics.txt").exists()
