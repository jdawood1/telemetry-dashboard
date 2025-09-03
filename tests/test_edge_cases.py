from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

from tlt.ingest import ingest_csv


def test_ingest_missing_columns(tmp_path: Path) -> None:
    csv = tmp_path / "bad.csv"
    # Missing feature_id
    pd.DataFrame(
        {
            "timestamp": ["2025-01-01T00:00:00Z"],
            "user_id": ["u1"],
            "event": ["click"],
        }
    ).to_csv(csv, index=False)

    with pytest.raises(ValueError) as ei:
        ingest_csv(csv, tmp_path / "out.parquet")

    assert "Missing required columns" in str(ei.value)


def test_ingest_bad_timestamp(tmp_path: Path) -> None:
    csv = tmp_path / "bad_ts.csv"
    pd.DataFrame(
        {
            "timestamp": ["not-a-time"],
            "user_id": ["u1"],
            "event": ["click"],
            "feature_id": ["f1"],
        }
    ).to_csv(csv, index=False)

    with pytest.raises(ValueError) as ei:
        ingest_csv(csv, tmp_path / "out.parquet")

    # your ingest raises on unparsable timestamps
    assert "timestamps could not be parsed" in str(ei.value).lower()
