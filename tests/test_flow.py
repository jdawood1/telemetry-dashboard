from pathlib import Path
from tlt.ingest import ingest_csv
from tlt.transform import transform_parquet
from tlt.report import make_reports

SAMPLE = Path("sample/events.csv")

def test_flow(tmp_path: Path):
    raw = tmp_path / "events.parquet"
    agg = tmp_path / "agg.parquet"
    out = tmp_path / "reports"

    ingest_csv(SAMPLE, raw)
    assert raw.exists()

    transform_parquet(raw, agg)
    assert agg.exists()

    make_reports(agg, out)
    assert (out / "feature_usage.png").exists()
    assert (out / "metrics.txt").exists()
