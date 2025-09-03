# Telemetry Dashboard — ingest → transform → report

[![CI](https://github.com/jdawood1/telemetry-dashboard/actions/workflows/ci.yml/badge.svg)](https://github.com/jdawood1/telemetry-dashboard/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pipeline to ingest raw events, compute daily aggregates (events, DAU, optional p50/p95 latency), and generate charts + metrics.  
Built as an end-to-end telemetry workflow prototype.

**Highlights**
- CSV → Parquet ingest with UTC-normalized timestamps
- Daily aggregates: events per (date, feature_id), DAU per day, optional p50/p95 latency
- Reports: feature-usage bar chart + metrics.txt summary
- CLI size command to compare CSV vs Parquet on-disk size
- Tested end-to-end (function calls + CLI) with pytest; edge cases covered

---

## Install

**Option A (recommended: via `pyproject.toml`)**
```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
pip install -e .[dev]   # installs package + dev tools (pytest/ruff/etc.)
```

**Option B (requirements.txt)**
```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
```

---

## Run in 30 seconds

```bash
python -m tlt.cli ingest --input sample/events.csv --out data/events.parquet
python -m tlt.cli transform --in data/events.parquet --out data/agg.parquet
python -m tlt.cli report --in data/agg.parquet --out reports/
python -m tlt.cli size --csv sample/events.csv --parquet data/events.parquet
```

**Outputs**
- `data/events.parquet` (raw events in Parquet)
- `data/agg.parquet` (aggregated metrics)
- `reports/feature_usage.png`, `reports/metrics.txt`

> The `size` command prints a small report to stdout (no file written).

---

## Requirements
- Python 3.11+
- Key deps: pandas, pyarrow, matplotlib, click (pytest/ruff/black as dev extras)
- Notes: timestamps are normalized to tz-aware **UTC** during ingest; matplotlib runs headless (**Agg**) in CI.

---

## Sample Chart
Below is an example chart generated from the pipeline:

![Feature usage](docs/feature_usage_sample.png)

---

## Sample Metrics (`reports/metrics.txt`)

```txt
Telemetry Summary:
- Aggregated days: 7
- Total events: 12,345
- Features: 6
- Mean DAU: 128.4
- Max DAU: 191
- Median latency p50 (overall mean): 51.2 ms
- Tail latency p95 (overall mean): 54.6 ms
```

---

## CLI

```bash
python -m tlt.cli --help
python -m tlt.cli ingest --help
python -m tlt.cli transform --help
python -m tlt.cli report --help
python -m tlt.cli size --help
```

If you installed with console scripts (`pip install -e .`), you can also run `tlt ...` instead of `python -m tlt.cli ...`.

---

## Tests

Run the test suite:
```bash
pytest -q
```

What’s covered:
- End-to-end happy path (`tests/test_flow.py`)
- CLI smoke test via subprocess (`tests/test_cli.py`)
- Edge cases for schema & timestamp parsing (`tests/test_edge_cases.py`)
- Size command (`tests/test_size.py`)

---

## Repo Structure
```
telemetry-dashboard/
  tlt/
    __init__.py
    cli.py         # CLI entrypoint (Click)
    ingest.py      # CSV -> Parquet (UTC-normalized timestamps)
    transform.py   # aggregates (events, DAU, optional p50/p95)
    report.py      # charts + metrics (feature usage, metrics.txt)
    size.py        # CSV vs Parquet size report (CLI: `size`)
  sample/
    events.csv     # sample dataset
  tests/
    test_flow.py       # end-to-end test
    test_cli.py        # CLI smoke test
    test_edge_cases.py # ingest schema/timestamp failures
    test_size.py       # size subcommand test
  scripts/
    add_latency.py     # helper to inject latency_ms into events
  docs/
    feature_usage_sample.png
  .github/workflows/ci.yml
  pyproject.toml   # or requirements.txt (if using Option B)
  pytest.ini
  README.md
```

---

## Roadmap
- [x] Add latency column to sample data and visualize p50/p95
- [ ] Add rolling 30d MAU
- [ ] Add per-feature latency distributions
- [x] Add CSV/Parquet size comparisons

---

## License
MIT © 2025 John Dawood

