"""Add project root to sys.path so `from traceset.xxx import ...` works.

Also provides the e2e fixture used by test_e2e.py.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest


@pytest.fixture(scope="session")
def ensure_langfuse_running():
    """Ensure Langfuse is running for e2e tests. Auto-starts Docker if needed.

    This fixture is NOT autouse — it only activates when an e2e test
    explicitly requests it. Unit tests (run with -m "not e2e") are
    unaffected.
    """
    from traceset.pipeline import load_config, check_health, ensure_langfuse

    config = load_config()
    if not check_health(config["langfuse_host"]):
        ensure_langfuse(config)
    assert check_health(config["langfuse_host"]), (
        "Langfuse is not healthy. Start it with: "
        "cd ../difyapp3 && docker compose up -d"
    )
    return config
