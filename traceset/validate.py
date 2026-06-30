#!/usr/bin/env python3
"""Validation layer: query Langfuse API and assert field-level correctness.

Uses urllib.request (stdlib only). Runs ~44 assertions per scenario.
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


def _api_get(url: str, public_key: str, secret_key: str) -> dict:
    """GET request with Basic auth. Returns parsed JSON."""
    credentials = base64.b64encode(
        f"{public_key}:{secret_key}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {credentials}"}
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def query_trace(
    trace_id: str, endpoint: str, public_key: str, secret_key: str
) -> dict:
    """GET /api/public/traces/{traceId}."""
    url = f"{endpoint.rstrip('/')}/api/public/traces/{trace_id}"
    return _api_get(url, public_key, secret_key)


def query_observations(
    trace_id: str, endpoint: str, public_key: str, secret_key: str
) -> list[dict]:
    """GET /api/public/observations?traceId={traceId}&limit=100."""
    url = (
        f"{endpoint.rstrip('/')}/api/public/observations"
        f"?traceId={trace_id}&limit=100"
    )
    result = _api_get(url, public_key, secret_key)
    if isinstance(result, dict) and "data" in result:
        return result["data"]
    if isinstance(result, list):
        return result
    return []


def wait_for_indexing(
    trace_id: str,
    expected_span_count: int,
    endpoint: str,
    public_key: str,
    secret_key: str,
    timeout: int = 30,
) -> None:
    """Poll until trace appears and observation count matches."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            trace = query_trace(trace_id, endpoint, public_key, secret_key)
            if trace and trace.get("id"):
                observations = query_observations(
                    trace_id, endpoint, public_key, secret_key
                )
                if len(observations) >= expected_span_count:
                    return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Trace {trace_id} did not index within {timeout}s "
        f"(expected {expected_span_count} observations)"
    )


def _assert_eq(name: str, actual, expected) -> dict:
    """Compare actual vs expected. Returns assertion result dict."""
    passed = actual == expected
    result = {
        "name": name,
        "passed": passed,
    }
    if not passed:
        result["actual"] = actual
        result["expected"] = expected
    return result


def validate_scenario(
    scenario,
    endpoint: str,
    public_key: str,
    secret_key: str,
) -> dict:
    """Run ~44 assertions for one scenario. Returns validation result dict."""
    events = scenario.build_events()
    meta = scenario.build_meta()

    trace_events = [e for e in events if e["type"] == "trace-create"]
    span_events = [
        e for e in events if e["type"] in ("span-create", "generation-create")
    ]

    trace_id = trace_events[0]["body"]["id"] if trace_events else None
    if not trace_id:
        return {
            "scenario_id": scenario.SCENARIO_ID,
            "assertions": [{"name": "trace_id", "passed": False}],
            "pass_count": 0,
            "fail_count": 1,
        }

    try:
        wait_for_indexing(
            trace_id,
            scenario.EXPECTED_SPAN_COUNT,
            endpoint,
            public_key,
            secret_key,
        )
    except (TimeoutError, Exception) as e:
        return {
            "scenario_id": scenario.SCENARIO_ID,
            "assertions": [
                {"name": "indexing", "passed": False, "error": str(e)}
            ],
            "pass_count": 0,
            "fail_count": 1,
        }

    trace = query_trace(trace_id, endpoint, public_key, secret_key)
    observations = query_observations(
        trace_id, endpoint, public_key, secret_key
    )

    assertions = []

    # Use the LAST trace-create event's body for name (upsert semantics — last write wins).
    # Use the FIRST trace-create event's body for userId/input/metadata (set on initial create).
    expected_trace_body_first = trace_events[0]["body"]
    expected_trace_body_last = trace_events[-1]["body"]
    assertions.append(_assert_eq("trace.id", trace.get("id"), expected_trace_body_first["id"]))
    assertions.append(_assert_eq("trace.name", trace.get("name"), expected_trace_body_last["name"]))
    assertions.append(_assert_eq("trace.userId", trace.get("userId"), expected_trace_body_first.get("userId")))
    assertions.append(_assert_eq("trace.input", trace.get("input"), expected_trace_body_first.get("input")))
    assertions.append(_assert_eq("trace.metadata", trace.get("metadata"), expected_trace_body_first.get("metadata")))

    obs_by_id = {o["id"]: o for o in observations}

    for event in span_events:
        body = event["body"]
        obs_id = body["id"]
        obs = obs_by_id.get(obs_id)

        if event["type"] == "generation-create":
            expected_type = "GENERATION"
        else:
            expected_type = "SPAN"

        actual_type = obs.get("type") if obs else None
        assertions.append(_assert_eq(
            f"obs.{obs_id}.type", actual_type, expected_type
        ))
        assertions.append(_assert_eq(
            f"obs.{obs_id}.input",
            obs.get("input") if obs else None,
            body.get("input"),
        ))
        assertions.append(_assert_eq(
            f"obs.{obs_id}.output",
            obs.get("output") if obs else None,
            body.get("output"),
        ))

        if event["type"] == "generation-create":
            assertions.append(_assert_eq(
                f"obs.{obs_id}.model",
                obs.get("model") if obs else None,
                body.get("model"),
            ))
            # Langfuse API returns `usage` field (not `usageDetails`).
            # The `usage` field has {input, output, total, unit} — no cost fields.
            # Cost fields are computed by Langfuse separately and won't match
            # our synthetic values, so we only compare the 4 core fields.
            wire_usage = body.get("usageDetails", {})
            expected_usage = {
                "input": wire_usage.get("input"),
                "output": wire_usage.get("output"),
                "total": wire_usage.get("total"),
                "unit": wire_usage.get("unit"),
            }
            actual_usage = obs.get("usage") if obs else None
            if actual_usage is None:
                actual_usage = {}
            actual_usage_subset = {
                "input": actual_usage.get("input"),
                "output": actual_usage.get("output"),
                "total": actual_usage.get("total"),
                "unit": actual_usage.get("unit"),
            }
            assertions.append(_assert_eq(
                f"obs.{obs_id}.usage",
                actual_usage_subset,
                expected_usage,
            ))
        else:
            assertions.append(_assert_eq(
                f"obs.{obs_id}.parentObservationId",
                obs.get("parentObservationId") if obs else None,
                body.get("parentObservationId"),
            ))

    assertions.append(_assert_eq(
        "obs.count", len(observations), scenario.EXPECTED_SPAN_COUNT
    ))

    obs_ids = {o["id"] for o in observations}
    orphan_count = sum(
        1 for o in observations
        if o.get("parentObservationId")
        and o["parentObservationId"] not in obs_ids
    )
    assertions.append(_assert_eq("obs.orphans", orphan_count, 0))

    # Check timestamp monotonicity in EXPECTED event order (not API return order).
    # Build expected order: map observation ID -> its position in span_events.
    expected_order = {body["id"]: idx for idx, event in enumerate(span_events) for body in [event["body"]]}
    # Sort actual observations by their expected position.
    ordered_obs = sorted(
        observations,
        key=lambda o: expected_order.get(o["id"], 999),
    )
    obs_times = [o.get("startTime", "") for o in ordered_obs]
    is_monotonic = all(
        obs_times[i] <= obs_times[i + 1] for i in range(len(obs_times) - 1)
    ) if len(obs_times) > 1 else True
    assertions.append(_assert_eq("obs.timestamps_monotonic", is_monotonic, True))

    pass_count = sum(1 for a in assertions if a["passed"])
    fail_count = sum(1 for a in assertions if not a["passed"])

    return {
        "scenario_id": scenario.SCENARIO_ID,
        "assertions": assertions,
        "assertion_count": len(assertions),
        "pass_count": pass_count,
        "fail_count": fail_count,
    }


def validate_all(
    scenarios: list,
    endpoint: str,
    public_key: str,
    secret_key: str,
) -> dict:
    """Validate all scenarios. Returns validation report dict."""
    results = []
    for scenario in scenarios:
        result = validate_scenario(scenario, endpoint, public_key, secret_key)
        results.append(result)
        status = "PASS" if result["fail_count"] == 0 else "FAIL"
        print(
            f"  {status} {scenario.SCENARIO_ID}: "
            f"{result['pass_count']}/{result['assertion_count']} assertions"
        )

    total_assertions = sum(r["assertion_count"] for r in results)
    total_pass = sum(r["pass_count"] for r in results)
    total_fail = sum(r["fail_count"] for r in results)

    return {
        "scenarios": results,
        "total_assertions": total_assertions,
        "total_pass": total_pass,
        "total_fail": total_fail,
    }


def write_validation_report(report: dict, path: str | None = None) -> str:
    """Write validation report to JSON file."""
    if path is None:
        path = os.path.join(_TRACESET_DIR, "validation_report.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return path


def main():
    from traceset.pipeline import load_config, ensure_langfuse

    config = load_config()
    ensure_langfuse(config)

    print("Validating scenarios against Langfuse...")
    report = validate_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )

    report_path = write_validation_report(report)
    print(f"\n  Validation report: {report_path}")

    print(f"\n{'Scenario':<30} {'Assertions':>12} {'Pass':>8} {'Fail':>8}")
    print("-" * 62)
    for s in report["scenarios"]:
        print(
            f"{s['scenario_id']:<30} {s['assertion_count']:>12} "
            f"{s['pass_count']:>8} {s['fail_count']:>8}"
        )
    print("-" * 62)
    print(
        f"{'Total':<30} {report['total_assertions']:>12} "
        f"{report['total_pass']:>8} {report['total_fail']:>8}"
    )
    if report["total_fail"] == 0:
        print("  ALL PASS")

    if report["total_fail"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
