from __future__ import annotations
from pathlib import Path

def _fmt(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    x = float(n); i = 0
    while x >= 1024 and i < len(units)-1:
        x /= 1024; i += 1
    return f"{x:.1f} {units[i]}"

def compare(csv_path: str | Path, parquet_path: str | Path) -> str:
    csv_p, pq_p = Path(csv_path), Path(parquet_path)
    if not csv_p.exists(): raise FileNotFoundError(csv_p)
    if not pq_p.exists(): raise FileNotFoundError(pq_p)
    c, p = csv_p.stat().st_size, pq_p.stat().st_size
    ratio = (p / c) if c else float("inf")
    return (
        "=== Size Comparison ===\n"
        f"CSV:     {_fmt(c)}  ({c} bytes)\n"
        f"Parquet: {_fmt(p)}  ({p} bytes)\n"
        f"Parquet/CSV ratio: {ratio:.3f}\n"
    )