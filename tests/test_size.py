from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def _py() -> str:
    return sys.executable


def test_size_cmd(tmp_path: Path) -> None:
    # minimal CSV + Parquet roundtrip through CLI
    csv = tmp_path / "x.csv"
    csv.write_text(
        "timestamp,user_id,event,feature_id\n2025-01-01T00:00:00Z,u1,open,f1\n", encoding="utf-8"
    )

    # ingest to get parquet
    events_parquet = tmp_path / "x.parquet"
    subprocess.check_call(
        [
            _py(),
            "-m",
            "tlt.cli",
            "ingest",
            "--input",
            str(csv),
            "--out",
            str(events_parquet),
        ]
    )
    out = subprocess.check_output(
        [
            _py(),
            "-m",
            "tlt.cli",
            "size",
            "--csv",
            str(csv),
            "--parquet",
            str(events_parquet),
        ],
        text=True,
    )
    assert "Size Comparison" in out
    assert "Parquet/CSV ratio" in out
