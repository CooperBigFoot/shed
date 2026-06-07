"""Regression coverage for benchmark trace flushing."""

import json
import os
from pathlib import Path

import pyshed


def test_bench_trace_writes_stage_records_before_process_exit(tmp_path):
    trace_path = tmp_path / "t.jsonl"
    fixture = (
        Path(__file__).resolve().parents[2]
        / "core"
        / "tests"
        / "fixtures"
        / "parity"
        / "v021_synthetic_refined"
    )

    with pyshed.bench_trace(trace_path):
        engine = pyshed.Engine(str(fixture), snap_threshold=500)
        engine.delineate(lat=-2.5, lon=2.5)

    assert os.path.getsize(trace_path) > 0
    records = [
        json.loads(line)
        for line in trace_path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert records
    assert all(record["kind"] == "stage" for record in records)
