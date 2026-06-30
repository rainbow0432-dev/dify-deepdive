#!/usr/bin/env python3
"""Pipeline orchestration: generate -> pack -> ingest -> validate.

Loads config from ../difyapp3/.env, health-checks Langfuse, auto-starts
Docker if needed, and orchestrates the 4-stage pipeline.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

from traceset.scenarios import SCENARIOS

_TRACESET_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_TRACESET_DIR)
_PARENT_DIR = os.path.dirname(_REPO_ROOT)
_DIFYAPP3_DIR = os.path.join(_PARENT_DIR, "difyapp3")
_ENV_PATH = os.path.join(_DIFYAPP3_DIR, ".env")


def load_config() -> dict:
    """Load Langfuse config from ../difyapp3/.env.

    Replaces host.docker.internal with localhost for host-side access.
    """
    config = {}
    with open(_ENV_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, _, value = line.partition("=")
                config[key.strip()] = value.strip()

    host = config.get("LANGFUSE_HOST", "http://localhost:3000")
    host = host.replace("host.docker.internal", "localhost")

    return {
        "langfuse_host": host,
        "langfuse_public_key": config.get("LANGFUSE_PUBLIC_KEY"),
        "langfuse_secret_key": config.get("LANGFUSE_SECRET_KEY"),
    }


def check_health(endpoint: str) -> bool:
    """Check if Langfuse is healthy via GET /api/public/health."""
    url = f"{endpoint.rstrip('/')}/api/public/health"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def ensure_langfuse(config: dict) -> None:
    """Ensure Langfuse is running. Auto-start Docker if needed."""
    endpoint = config["langfuse_host"]

    if check_health(endpoint):
        return

    print("Langfuse not healthy. Attempting to start Docker...")
    try:
        subprocess.run(
            [
                "docker", "compose",
                "-f", "docker-compose.yaml",
                "-f", "docker-compose.override.yml",
                "up", "-d",
            ],
            cwd=_DIFYAPP3_DIR,
            check=True,
            capture_output=True,
            timeout=60,
        )
    except FileNotFoundError:
        print(
            "Docker is not installed. Please install Docker or start Langfuse manually.",
            file=sys.stderr,
        )
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(
            f"Failed to start Docker: {e.stderr.decode() if e.stderr else e}",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Waiting for Langfuse to become healthy...")
    deadline = time.time() + 120
    while time.time() < deadline:
        if check_health(endpoint):
            print("Langfuse is healthy.")
            return
        time.sleep(2)

    print("Langfuse did not become healthy within 120s.", file=sys.stderr)
    sys.exit(1)


def clean_traces(config: dict) -> None:
    """Delete all scenario traces from Langfuse before ingesting."""
    endpoint = config["langfuse_host"]
    public_key = config["langfuse_public_key"]
    secret_key = config["langfuse_secret_key"]
    credentials = base64.b64encode(
        f"{public_key}:{secret_key}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {credentials}"}

    for scenario in SCENARIOS:
        events = scenario.build_events()
        trace_id = events[0]["body"]["id"]
        url = f"{endpoint.rstrip('/')}/api/public/traces/{trace_id}"
        req = urllib.request.Request(url, headers=headers, method="DELETE")
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass

    print(f"Cleaned {len(SCENARIOS)} traces.")


def run_generate() -> None:
    """Stage 1: Generate events.jsonl and meta.json for all scenarios."""
    from traceset.generate_traceset import main as gen_main

    print("\n=== Stage 1: Generate ===")
    gen_main()


def run_ingest(config: dict) -> dict:
    """Stage 2+3: Pack and ingest all scenarios."""
    from traceset.ingest import ingest_all, write_ingestion_report

    print("\n=== Stage 2+3: Pack + Ingest ===")
    report = ingest_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    write_ingestion_report(report)
    print(
        f"  Ingested: {report['total_successes']} successes, "
        f"{report['total_errors']} errors"
    )
    return report


def run_validate(config: dict) -> dict:
    """Stage 4: Validate all scenarios against Langfuse."""
    from traceset.validate import validate_all, write_validation_report

    print("\n=== Stage 4: Validate ===")
    report = validate_all(
        SCENARIOS,
        config["langfuse_host"],
        config["langfuse_public_key"],
        config["langfuse_secret_key"],
    )
    write_validation_report(report)
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Traceset v2 E2E Pipeline: generate -> ingest -> validate"
    )
    parser.add_argument(
        "--stage",
        choices=["generate", "ingest", "validate", "all"],
        default="all",
        help="Which stage to run (default: all)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing traces before ingesting",
    )
    args = parser.parse_args()

    config = load_config()
    ensure_langfuse(config)

    if args.clean:
        clean_traces(config)

    if args.stage in ("generate", "all"):
        run_generate()

    if args.stage in ("ingest", "all"):
        ingest_report = run_ingest(config)
        if ingest_report["total_errors"] > 0:
            print(
                f"WARNING: {ingest_report['total_errors']} ingestion errors",
                file=sys.stderr,
            )

    if args.stage in ("validate", "all"):
        val_report = run_validate(config)
        print(f"\nTotal: {val_report['total_pass']} pass, "
              f"{val_report['total_fail']} fail")
        if val_report["total_fail"] > 0:
            sys.exit(1)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
