#!/usr/bin/env python3
"""Ingestion layer: pack events into Langfuse batch payloads and POST via HTTP.

Uses urllib.request (stdlib only — no requests dependency).
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

from traceset.scenarios import SCENARIOS

_TRACESET_DIR = os.path.dirname(os.path.abspath(__file__))


def pack_batch(events: list[dict]) -> dict:
    """Pack a list of events into a Langfuse ingestion batch payload."""
    return {"batch": events}


def post_batch(
    batch: dict,
    endpoint: str,
    public_key: str,
    secret_key: str,
    max_retries: int = 3,
) -> dict:
    """POST a batch to Langfuse /api/public/ingestion.

    Returns parsed JSON response with _http_status added.
    Raises RuntimeError on unrecoverable failures.
    """
    url = f"{endpoint.rstrip('/')}/api/public/ingestion"
    body = json.dumps(batch).encode("utf-8")
    credentials = base64.b64encode(
        f"{public_key}:{secret_key}".encode()
    ).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {credentials}",
    }

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, data=body, headers=headers, method="POST"
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status
                resp_body = json.loads(resp.read().decode("utf-8"))
                resp_body["_http_status"] = status
                return resp_body
        except urllib.error.HTTPError as e:
            resp_text = e.read().decode("utf-8")
            if e.code == 207:
                resp_body = json.loads(resp_text)
                resp_body["_http_status"] = 207
                return resp_body
            elif 400 <= e.code < 500:
                raise RuntimeError(
                    f"HTTP {e.code} from Langfuse: {resp_text}"
                ) from e
            elif 500 <= e.code < 600:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                raise RuntimeError(
                    f"HTTP {e.code} after {max_retries} retries: {resp_text}"
                ) from e
        except urllib.error.URLError as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            raise RuntimeError(f"Network error: {e}") from e

    raise RuntimeError(f"Max retries ({max_retries}) exceeded")


def ingest_all(
    scenarios: list,
    endpoint: str,
    public_key: str,
    secret_key: str,
    batch_mode: str = "per-scenario",
) -> dict:
    """Ingest all scenarios. Returns ingestion report dict."""
    report = {
        "scenarios": [],
        "total_successes": 0,
        "total_errors": 0,
        "batch_mode": batch_mode,
    }

    for scenario in scenarios:
        events = scenario.build_events()
        scenario_id = scenario.SCENARIO_ID
        trace_id = events[0]["body"]["id"] if events else None

        if batch_mode == "per-scenario":
            batch = pack_batch(events)
            try:
                result = post_batch(
                    batch, endpoint, public_key, secret_key
                )
                successes = len(result.get("success", []))
                errors = len(result.get("errors", []))
                report["scenarios"].append({
                    "scenario_id": scenario_id,
                    "trace_id": trace_id,
                    "http_status": result.get("_http_status"),
                    "success_count": successes,
                    "error_count": errors,
                    "errors": result.get("errors", []),
                })
                report["total_successes"] += successes
                report["total_errors"] += errors
            except RuntimeError as e:
                report["scenarios"].append({
                    "scenario_id": scenario_id,
                    "trace_id": trace_id,
                    "http_status": None,
                    "success_count": 0,
                    "error_count": len(events),
                    "errors": [{"message": str(e)}],
                })
                report["total_errors"] += len(events)

        elif batch_mode == "per-event":
            for i, event in enumerate(events):
                batch = pack_batch([event])
                try:
                    result = post_batch(
                        batch, endpoint, public_key, secret_key
                    )
                    report["total_successes"] += len(
                        result.get("success", [])
                    )
                    report["total_errors"] += len(
                        result.get("errors", [])
                    )
                except RuntimeError as e:
                    report["total_errors"] += 1
        else:
            raise ValueError(f"Unknown batch_mode: {batch_mode}")

    return report


def write_ingestion_report(report: dict, path: str | None = None) -> str:
    """Write ingestion report to JSON file. Returns path."""
    if path is None:
        path = os.path.join(_TRACESET_DIR, "ingestion_report.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def main():
    from traceset.pipeline import load_config, ensure_langfuse

    config = load_config()
    ensure_langfuse(config)

    print("Ingesting scenarios into Langfuse...")
    report = ingest_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )

    report_path = write_ingestion_report(report)
    print(f"  Ingestion report: {report_path}")
    print(
        f"  Total: {report['total_successes']} successes, "
        f"{report['total_errors']} errors"
    )

    if report["total_errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
