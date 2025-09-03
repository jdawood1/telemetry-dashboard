from __future__ import annotations

from pathlib import Path
from inspect import signature
import click

from .ingest import ingest_csv
from .transform import transform_parquet
from .report import make_reports
from . import size as size_mod

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


def _call_with_supported_args(func, /, *args, **kwargs):
    """Call func with only the kwargs it supports (safe pass-through)."""
    params = set(signature(func).parameters)
    filtered = {k: v for k, v in kwargs.items() if k in params}
    return func(*args, **filtered)


@click.group(
    help="Telemetry pipeline CLI: ingest → transform → report.",
    context_settings=CONTEXT_SETTINGS,
)
@click.version_option(package_name="telemetry-dashboard")
def cli() -> None:
    pass


@cli.command("ingest", short_help="CSV → Parquet")
@click.option(
    "--input", "-i",
    "input_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input CSV with columns: timestamp,user_id,event,feature_id[,latency_ms]",
)
@click.option(
    "--out", "-o",
    "out_path",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output Parquet path (parent dir will be created).",
)
def ingest_cmd(input_path: Path, out_path: Path) -> None:
    """Read CSV → write Parquet."""
    try:
        p = _call_with_supported_args(ingest_csv, input_path, out_path)
        click.echo(f"Wrote: {p}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command("transform", short_help="Aggregate daily metrics")
@click.option(
    "--in", "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input Parquet from ingest step.",
)
@click.option(
    "--out", "out_path",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output aggregated Parquet (parent dir will be created).",
)
@click.option(
    "--mau-window",
    type=int,
    default=30,
    show_default=True,
    help="Rolling MAU window (days). Forwarded if supported.",
)
def transform_cmd(in_path: Path, out_path: Path, mau_window: int) -> None:
    """Aggregate metrics: events/day+feature, DAU/day, optional p50/p95 latency, and MAU."""
    try:
        p = _call_with_supported_args(
            transform_parquet,
            in_path, out_path,
            mau_window=mau_window,
        )
        click.echo(f"Wrote: {p}")
    except Exception as e:
        raise click.ClickException(str(e)) from e


@cli.command("report", short_help="Generate charts + metrics")
@click.option(
    "--in", "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input aggregated Parquet from transform step.",
)
@click.option(
    "--out", "out_dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Output directory for reports (metrics.txt, charts). Will be created.",
)
@click.option(
    "--events",
    "events_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    help="(Optional) Raw events Parquet for per-feature latency plots and better latency stats.",
)
def report_cmd(in_path: Path, out_dir: Path, events_path: Path | None) -> None:
    """Generate text and chart reports from aggregated Parquet."""
    try:
        p = _call_with_supported_args(make_reports, in_path, out_dir, events_path=events_path)
        click.echo(f"Wrote reports to: {p}")
    except Exception as e:
        raise click.ClickException(str(e)) from e

@cli.command("size", short_help="Compare CSV vs Parquet sizes")
@click.option("--csv", "csv_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--parquet", "parquet_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
def size_cmd(csv_path: Path, parquet_path: Path) -> None:
    try:
        txt = size_mod.compare(csv_path, parquet_path)
        click.echo(txt)
    except Exception as e:
        raise click.ClickException(str(e)) from e

if __name__ == "__main__":
    cli()
