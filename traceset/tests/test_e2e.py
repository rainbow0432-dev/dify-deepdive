"""E2E tests against real Langfuse. Auto-starts Docker if needed.

Run with: python3 -m pytest traceset/tests/test_e2e.py -v -m e2e
Skip with: python3 -m pytest traceset/ -v -m "not e2e"
"""
import pytest

from traceset.scenarios import SCENARIOS
from traceset.ingest import pack_batch, post_batch, ingest_all
from traceset.validate import wait_for_indexing, validate_scenario, validate_all


@pytest.mark.e2e
def test_e2e_full_pipeline(ensure_langfuse_running):
    """Run the complete pipeline: generate -> ingest -> validate.

    All ~570 assertions across 13 scenarios must pass.
    """
    config = ensure_langfuse_running

    # 1. Generate
    from traceset.generate_traceset import main as gen_main
    gen_main()

    # 2. Ingest all scenarios
    report = ingest_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert report["total_errors"] == 0, (
        f"Ingestion had {report['total_errors']} errors"
    )

    # 3. Validate all scenarios
    val_report = validate_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert val_report["total_fail"] == 0, (
        f"Validation had {val_report['total_fail']} failures out of "
        f"{val_report['total_assertions']} assertions"
    )


@pytest.mark.e2e
@pytest.mark.parametrize(
    "scenario",
    SCENARIOS,
    ids=[s.SCENARIO_ID for s in SCENARIOS],
)
def test_e2e_scenario(scenario, ensure_langfuse_running):
    """Per-scenario e2e: ingest one scenario's events, then validate.

    Each scenario must pass all its assertions (~44 per scenario).
    """
    config = ensure_langfuse_running

    # 1. Build events
    events = scenario.build_events()
    assert len(events) == scenario.EXPECTED_EVENT_COUNT

    # 2. Pack and POST
    batch = pack_batch(events)
    result = post_batch(
        batch,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert result.get("_http_status") in (200, 207), (
        f"HTTP status {result.get('_http_status')} for {scenario.SCENARIO_ID}"
    )

    # 3. Wait for indexing
    trace_id = None
    for e in events:
        if e["type"] == "trace-create":
            trace_id = e["body"]["id"]
            break
    assert trace_id is not None

    wait_for_indexing(
        trace_id,
        scenario.EXPECTED_SPAN_COUNT,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )

    # 4. Validate
    val_result = validate_scenario(
        scenario,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    assert val_result["fail_count"] == 0, (
        f"Scenario {scenario.SCENARIO_ID} had {val_result['fail_count']} "
        f"validation failures:\n"
        + "\n".join(
            f"  FAIL: {a['name']}" for a in val_result["assertions"] if not a["passed"]
        )
    )
