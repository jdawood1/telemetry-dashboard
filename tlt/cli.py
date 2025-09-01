from pathlib import Path
import click

from .ingest import ingest_csv
from .transform import transform_parquet
from .report import make_reports


@click.group()
def cli():
    """Telemetry pipeline CLI: ingest -> transform -> report."""
    pass


@cli.command("ingest")
@click.option(
    "--input", "input_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input CSV with columns: timestamp,user_id,event,feature_id[,latency_ms]",
)
@click.option(
    "--out", "out_path",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output parquet path (parent dir will be created).",
)
def ingest_cmd(input_path: Path, out_path: Path):
    """Read CSV -> write parquet."""
    try:
        p = ingest_csv(input_path, out_path)
        click.echo(f"Wrote: {p}")
    except Exception as e:
        raise click.ClickException(str(e))


@cli.command("transform")
@click.option(
    "--in", "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input parquet from ingest step.",
)
@click.option(
    "--out", "out_path",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output aggregated parquet (parent dir will be created).",
)
def transform_cmd(in_path: Path, out_path: Path):
    """Aggregate metrics: events per (date,feature), DAU per date, optional p50/p95 latency."""
    try:
        p = transform_parquet(in_path, out_path)
        click.echo(f"Wrote: {p}")
    except Exception as e:
        raise click.ClickException(str(e))


@cli.command("report")
@click.option(
    "--in", "in_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Input aggregated parquet from transform step.",
)
@click.option(
    "--out", "out_dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Output directory for reports (metrics.txt, charts). Will be created.",
)
def report_cmd(in_path: Path, out_dir: Path):
    """Generate text and chart reports from aggregated parquet."""
    try:
        p = make_reports(in_path, out_dir)
        click.echo(f"Wrote reports to: {p}")
    except Exception as e:
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
