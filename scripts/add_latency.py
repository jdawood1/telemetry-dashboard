#!/usr/bin/env python3
from __future__ import annotations

import csv
import random
import sys
from pathlib import Path


def add_latency(inp: str | Path, outp: str | Path) -> Path:
    inp, outp = Path(inp), Path(outp)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with inp.open(newline="") as f_in, outp.open("w", newline="") as f_out:
        reader = csv.DictReader(f_in)
        fieldnames = list(reader.fieldnames or [])
        if "latency_ms" not in fieldnames:
            fieldnames.append("latency_ms")

        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            feat = row.get("feature_id", "")

            # simple feature-specific latency model (ms)
            if feat in {"menu", "inventory"}:
                base = 30
            elif feat in {"matchmake", "level_load"}:
                base = 60
            else:
                base = 45

            jitter = int(abs(random.gauss(0, 10)))
            row["latency_ms"] = str(base + jitter)
            writer.writerow(row)

    return outp


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("Usage: python scripts/add_latency.py <in.csv> <out.csv>")
        return 2
    add_latency(argv[1], argv[2])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
