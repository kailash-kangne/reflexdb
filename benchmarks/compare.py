#!/usr/bin/env python3
"""Compare benchmark history rows (from run.py --output) across labeled builds.

Groups rows by (workload, pipeline, clients), then for each group compares the
newest label's median ops/sec and p99 latency against a baseline label. Rows
without a label compare chronologically (latest run vs the run before it).

Usage:
    python3 benchmarks/compare.py history.jsonl                 # auto: newest label vs previous
    python3 benchmarks/compare.py history.jsonl --base main --head feature
    python3 benchmarks/compare.py history.jsonl --threshold 5   # flag |delta| > 5% (default 10)
"""
import argparse
import json
import statistics
import sys
from collections import defaultdict


def load_rows(path):
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"warning: skipping malformed line {i}", file=sys.stderr)


def group_key(row):
    return (row.get("workload", "?"), row.get("pipeline", 1), row.get("clients", 0))


def med(rows, field):
    vals = [r[field] for r in rows if isinstance(r.get(field), (int, float))]
    return statistics.median(vals) if vals else None


def p99(row_rows):
    vals = [r["latency_us"]["p99"] for r in row_rows
            if isinstance(r.get("latency_us"), dict) and "p99" in r["latency_us"]]
    return statistics.median(vals) if vals else None


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("history", help="JSONL file written by run.py --output")
    ap.add_argument("--base", help="baseline label (default: second-newest label present)")
    ap.add_argument("--head", help="candidate label (default: newest label present)")
    ap.add_argument("--threshold", type=float, default=10.0)
    args = ap.parse_args()

    groups = defaultdict(lambda: defaultdict(list))
    newest_ts = defaultdict(int)
    for row in load_rows(args.history):
        label = row.get("label", "")
        groups[group_key(row)][label].append(row)
        newest_ts[label] = max(newest_ts[label], row.get("timestamp", 0))

    if not groups:
        sys.exit(f"no benchmark rows in {args.history}")

    labels = sorted(newest_ts, key=lambda l: (newest_ts[l], l))
    if args.head is None:
        if len(labels) < 2:
            sys.exit("need at least two distinct --label runs to compare "
                     "(rerun run.py with --label <sha>)")
        args.head = labels[-1]
    if args.base is None:
        older = [l for l in labels if l != args.head]
        if not older:
            sys.exit(f"only label {args.head!r} present; nothing to compare against")
        args.base = older[-1]

    flagged = 0
    hdr = f"{'workload':<14} {'pipe':>4} {'cli':>4} {'base ops/s':>12} {'head ops/s':>12} {'delta':>7} {'base p99us':>10} {'head p99us':>10}"
    print(f"base={args.base or '(unlabeled)'}  head={args.head or '(unlabeled)'}  threshold=±{args.threshold}%")
    print(hdr)
    for key in sorted(groups):
        base_rows = groups[key].get(args.base, [])
        head_rows = groups[key].get(args.head, [])
        if not base_rows or not head_rows:
            continue
        b_ops, h_ops = med(base_rows, "ops_per_second"), med(head_rows, "ops_per_second")
        b_p99, h_p99 = p99(base_rows), p99(head_rows)
        if not b_ops or not h_ops:
            continue
        delta = (h_ops - b_ops) / b_ops * 100
        mark = ""
        if abs(delta) > args.threshold:
            mark = " IMPROVED" if delta > 0 else " REGRESSED"
            flagged += 1
        name, pipe, cli = key
        print(f"{name:<14} {pipe:>4} {cli:>4} {b_ops:>12.0f} {h_ops:>12.0f} {delta:>+6.1f}% "
              f"{(b_p99 or 0):>10.0f} {(h_p99 or 0):>10.0f}{mark}")

    sys.exit(1 if flagged else 0)


if __name__ == "__main__":
    main()
