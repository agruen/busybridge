"""FastAPI dashboard server for soak test results."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from sidecar.framework.base import TestResult

app = FastAPI(title="BusyBridge Test Sidecar")

# Shared state (set by main.py before starting)
_state: dict = {
    "results": [],
    "start_time": time.time(),
    "runner": None,
    "log_dir": "/data/test-logs",
}


def set_state(key: str, value) -> None:
    _state[key] = value


def get_results() -> list[TestResult]:
    return _state["results"]


def add_result(result: TestResult) -> None:
    _state["results"].append(result)
    _append_to_log(result)


def _append_to_log(result: TestResult) -> None:
    log_dir = Path(_state["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"results-{date_str}.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps(result.to_dict()) + "\n")


def load_today_results() -> list[TestResult]:
    """Load today's results from JSONL file on startup."""
    log_dir = Path(_state["log_dir"])
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"results-{date_str}.jsonl"
    results = []
    if log_file.exists():
        for line in log_file.read_text().splitlines():
            if line.strip():
                try:
                    d = json.loads(line)
                    from sidecar.framework.base import TestStatus
                    results.append(TestResult(
                        test_name=d["test_name"],
                        suite=d["suite"],
                        status=TestStatus(d["status"]),
                        duration=d["duration"],
                        run_id=d["run_id"],
                        timestamp=d["timestamp"],
                        error=d.get("error"),
                        details=d.get("details", {}),
                    ))
                except Exception:
                    pass
    return results


@app.get("/health")
async def health():
    runner = _state.get("runner")
    return {
        "status": "ok",
        "uptime": round(time.time() - _state["start_time"]),
        "tests_run": len(_state["results"]),
        "current_test": runner.current_test if runner else None,
    }


@app.get("/api/summary")
async def summary():
    results = _state["results"]
    total = len(results)
    passed = sum(1 for r in results if r.status.value == "passed")
    failed = sum(1 for r in results if r.status.value == "failed")
    errored = sum(1 for r in results if r.status.value == "error")
    skipped = sum(1 for r in results if r.status.value == "skipped")
    error_rate = round((failed + errored) / total * 100, 1) if total else 0

    by_suite: dict = {}
    for r in results:
        s = by_suite.setdefault(r.suite, {"total": 0, "passed": 0, "failed": 0, "error": 0})
        s["total"] += 1
        if r.status.value == "passed":
            s["passed"] += 1
        elif r.status.value == "failed":
            s["failed"] += 1
        else:
            s["error"] += 1

    runner = _state.get("runner")
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "errored": errored,
        "skipped": skipped,
        "error_rate": error_rate,
        "by_suite": by_suite,
        "uptime": round(time.time() - _state["start_time"]),
        "current_test": runner.current_test if runner else None,
    }


@app.get("/api/results")
async def list_results(
    status: Optional[str] = Query(None),
    suite: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    results = list(reversed(_state["results"]))  # newest first
    if status:
        results = [r for r in results if r.status.value == status]
    if suite:
        results = [r for r in results if r.suite == suite]
    total = len(results)
    page = results[offset : offset + limit]
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "results": [r.to_dict() for r in page],
    }


@app.get("/api/results/{run_id}")
async def get_result(run_id: str):
    for r in _state["results"]:
        if r.run_id == run_id:
            return r.to_dict()
    return {"error": "not found"}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    template_path = Path(__file__).parent / "templates" / "index.html"
    return template_path.read_text()
