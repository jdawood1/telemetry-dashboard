# Telemetry Dashboard — ingest → transform → report

[![CI](https://github.com/jdawood1/telemetry-dashboard/actions/workflows/ci.yml/badge.svg)](https://github.com/jdawood1/telemetry-dashboard/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pipeline to ingest raw events, compute daily aggregates (events, DAU, optional p50/p95 latency), and generate charts + metrics.  
Built as an end-to-end telemetry workflow prototype.

---

## Run in 30 seconds

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt

python -m tlt.cli ingest --input sample/events.csv --out data/events.parquet
python -m tlt.cli transform --in data/events.parquet --out data/agg.parquet
python -m tlt.cli report --in data/agg.parquet --out reports/
```

**Outputs**
- `data/events.parquet` (raw events in parquet)
- `data/agg.parquet` (aggregated metrics)
- `reports/feature_usage.png`, `reports/metrics.txt`

---

## Requirements
- Python 3.11+
- See `requirements.txt` for dependencies:
  - pandas, pyarrow, matplotlib, click, pytest

---

## Sample Chart
Below is an example chart generated from the pipeline:

![Feature usage](docs/feature_usage_sample.png)

---

## Sample Metrics (`reports/metrics.txt`)

```txt
=== Telemetry Summary ===
Aggregated days: 7
Total events: 12,345
Features: 6
Mean DAU: 128.4
Max DAU: 191
Median latency p50 (overall mean): 51.2 ms
Tail latency p95 (overall mean): 54.6 ms
```

---

## CLI

```bash
python -m tlt.cli --help
python -m tlt.cli ingest --help
python -m tlt.cli transform --help
python -m tlt.cli report --help
```

---

## Repo Structure
```
telemetry-dashboard/
  tlt/
    cli.py         # CLI entrypoint (Click)
    ingest.py      # CSV -> parquet
    transform.py   # aggregates (events, DAU, optional p50/p95)
    report.py      # charts + metrics
  sample/
    events.csv     # sample dataset
  tests/
    test_flow.py   # end-to-end test
  scripts/
    add_latency.py # helper to inject latency_ms into events
  docs/
    feature_usage_sample.png
  .github/workflows/ci.yml
  requirements.txt
  README.md
```

---

## Roadmap
- [x] Add latency column to sample data and visualize p50/p95
- [ ] Add rolling 30d MAU
- [ ] Add per-feature latency distributions
- [ ] Add CSV/Parquet size comparisons

---

## License
MIT © 2025 John Dawood
