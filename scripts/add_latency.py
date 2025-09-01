import sys, random, csv

inp, outp = sys.argv[1], sys.argv[2]
with open(inp, newline="") as f, open(outp, "w", newline="") as g:
    r = csv.DictReader(f)
    fieldnames = r.fieldnames + (["latency_ms"] if "latency_ms" not in r.fieldnames else [])
    w = csv.DictWriter(g, fieldnames=fieldnames)
    w.writeheader()
    for row in r:
        feat = row.get("feature_id", "")
        # simple feature-specific latency model (in ms)
        base = 30 if feat in {"menu", "inventory"} else 60 if feat in {"matchmake", "level_load"} else 45
        jitter = int(abs(random.gauss(0, 10)))
        row["latency_ms"] = str(base + jitter)
        w.writerow(row)
