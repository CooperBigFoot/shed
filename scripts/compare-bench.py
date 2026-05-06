#!/usr/bin/env python3
"""Compare two bench_delineate JSONL files."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path


HELP_GATES = """\
Gate JSON format:
{
  "max_wall_pct_regression": 10.0,
  "max_pct_regression_by_stage": {
    "watershed_assembly": 15.0,
    "*": 25.0
  }
}
Percent regression is positive when candidate is slower than baseline.
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print markdown deltas for bench_delineate JSONL outputs.",
        epilog=HELP_GATES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--gates", type=Path, help="optional regression gates JSON")
    args = parser.parse_args()

    baseline = load(args.baseline)
    candidate = load(args.candidate)
    gates = load_gates(args.gates)

    print_stage_table(baseline["stages"], candidate["stages"])
    print()
    print_wall_delta(baseline["wall"], candidate["wall"])

    failures = check_gates(baseline, candidate, gates)
    if failures:
        print("\nGate failures:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


def load(path: Path) -> dict:
    stages: dict[str, list[float]] = defaultdict(list)
    wall: list[float] = []
    with path.open() as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            kind = record.get("kind")
            if kind == "stage" and "stage" in record and "duration_ms" in record:
                stages[record["stage"]].append(float(record["duration_ms"]))
            elif kind == "iteration" and "wall_time_ms" in record:
                wall.append(float(record["wall_time_ms"]))
    return {"stages": stages, "wall": wall}


def load_gates(path: Path | None) -> dict:
    if path is None:
        return {}
    with path.open() as handle:
        return json.load(handle)


def print_stage_table(baseline: dict[str, list[float]], candidate: dict[str, list[float]]) -> None:
    rows = []
    for stage in sorted(set(baseline) | set(candidate)):
        base = median(baseline.get(stage, []))
        cand = median(candidate.get(stage, []))
        delta = none_if_missing(cand, base, lambda c, b: c - b)
        pct = percent_delta(cand, base)
        rows.append((abs(delta or 0.0), stage, base, cand, delta, pct))

    rows.sort(reverse=True)
    print("| stage | baseline median ms | candidate median ms | delta ms | delta % |")
    print("|---|---:|---:|---:|---:|")
    for _, stage, base, cand, delta, pct in rows:
        print(
            f"| {stage} | {fmt(base)} | {fmt(cand)} | {fmt(delta)} | {fmt_pct(pct)} |"
        )


def print_wall_delta(baseline: list[float], candidate: list[float]) -> None:
    base = median(baseline)
    cand = median(candidate)
    delta = none_if_missing(cand, base, lambda c, b: c - b)
    pct = percent_delta(cand, base)
    print("| metric | baseline median ms | candidate median ms | delta ms | delta % |")
    print("|---|---:|---:|---:|---:|")
    print(f"| wall_time | {fmt(base)} | {fmt(cand)} | {fmt(delta)} | {fmt_pct(pct)} |")


def check_gates(baseline: dict, candidate: dict, gates: dict) -> list[str]:
    failures = []
    wall_gate = gates.get("max_wall_pct_regression")
    wall_pct = percent_delta(median(candidate["wall"]), median(baseline["wall"]))
    if wall_gate is not None and wall_pct is not None and wall_pct > float(wall_gate):
        failures.append(f"wall regression {wall_pct:.2f}% exceeds {float(wall_gate):.2f}%")

    stage_gates = gates.get("max_pct_regression_by_stage", {})
    default_stage_gate = stage_gates.get("*")
    for stage in sorted(set(baseline["stages"]) | set(candidate["stages"])):
        gate = stage_gates.get(stage, default_stage_gate)
        if gate is None:
            continue
        pct = percent_delta(
            median(candidate["stages"].get(stage, [])),
            median(baseline["stages"].get(stage, [])),
        )
        if pct is not None and pct > float(gate):
            failures.append(f"{stage} regression {pct:.2f}% exceeds {float(gate):.2f}%")
    return failures


def median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0


def percent_delta(candidate: float | None, baseline: float | None) -> float | None:
    if candidate is None or baseline is None or math.isclose(baseline, 0.0):
        return None
    return ((candidate - baseline) / baseline) * 100.0


def none_if_missing(candidate: float | None, baseline: float | None, fn):
    if candidate is None or baseline is None:
        return None
    return fn(candidate, baseline)


def fmt(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}%"


if __name__ == "__main__":
    raise SystemExit(main())
