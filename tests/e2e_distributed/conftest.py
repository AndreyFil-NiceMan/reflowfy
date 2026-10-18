"""Fixtures for the distributed E2E suite.

The suite talks to a *running* Reflowfy deployment in distributed mode (manager + Kafka +
a worker pool), not to a stack it starts itself. Point it at one with environment
variables (defaults in parentheses):

    E2E_DIST_MANAGER_URL        manager REST API as the test runner sees it (http://localhost:8001)
    E2E_DIST_SINK_URL           the sink as the test runner sees it         (http://localhost:8090)
    E2E_DIST_SINK_INTERNAL_URL  the sink as the *workers* see it            (http://e2e-sink:8080)
    E2E_DIST_ES_INTERNAL_URL    Elasticsearch as the workers see it         (http://es:9200)
    E2E_DIST_ES=1               also run the Elasticsearch tests (index "docs" with seq 1..N)
    E2E_DIST_KILL_WORKER_CMD    shell command that force-kills one worker   (enables @chaos worker tests)
    E2E_DIST_RESTART_MANAGER_CMD  shell command that force-kills the manager (enables @chaos manager tests)

The pipelines in ``pipelines/dist_pipelines.py`` must be built into the deployment's images.
See README.md.
"""

import os
import subprocess
import time
import uuid
from typing import Any, Callable, Dict, Iterator

import httpx
import pytest

from tests.e2e_distributed.helpers import Manager, Sink

MANAGER_URL = os.getenv("E2E_DIST_MANAGER_URL", "http://localhost:8001")
SINK_URL = os.getenv("E2E_DIST_SINK_URL", "http://localhost:8090")
SINK_INTERNAL_URL = os.getenv("E2E_DIST_SINK_INTERNAL_URL", "http://e2e-sink:8080")
ES_INTERNAL_URL = os.getenv("E2E_DIST_ES_INTERNAL_URL", "http://es:9200")
KILL_WORKER_CMD = os.getenv("E2E_DIST_KILL_WORKER_CMD", "")
RESTART_MANAGER_CMD = os.getenv("E2E_DIST_RESTART_MANAGER_CMD", "")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "chaos: kills pods; needs E2E_DIST_*_CMD to be set")
    config.addinivalue_line("markers", "slow: takes more than ~2 minutes")
    config.addinivalue_line("markers", "needs_es: needs Elasticsearch (E2E_DIST_ES=1)")
    config.addinivalue_line(
        "markers", "characterization: pins down current behaviour that may be a bug — see the test"
    )


def pytest_collection_modifyitems(config: pytest.Config, items: Any) -> None:
    for item in items:
        if "needs_es" in item.keywords and os.getenv("E2E_DIST_ES") != "1":
            item.add_marker(pytest.mark.skip(reason="set E2E_DIST_ES=1 (needs Elasticsearch)"))


@pytest.fixture(scope="session", autouse=True)
def _deployment_is_reachable() -> None:
    """Skip the whole suite, with the reason, when there is nothing to test against."""
    try:
        manager = httpx.get(f"{MANAGER_URL}/health", timeout=5)
        sink = httpx.get(f"{SINK_URL}/health", timeout=5)
    except httpx.HTTPError as exc:
        pytest.skip(f"no distributed deployment reachable ({exc}); see tests/e2e_distributed/README.md")
    if manager.status_code != 200 or sink.status_code != 200:
        pytest.skip(f"manager {manager.status_code} / sink {sink.status_code}: not healthy")
    names = {p["name"] for p in httpx.get(f"{MANAGER_URL}/pipelines", timeout=10).json()}
    if "dist_abstract" not in names:
        pytest.skip("pipelines not deployed: build tests/e2e_distributed/pipelines into the images")


@pytest.fixture(scope="session")
def manager() -> Manager:
    return Manager(MANAGER_URL)


@pytest.fixture(scope="session")
def sink() -> Sink:
    return Sink(SINK_URL)


@pytest.fixture
def channel(request: pytest.FixtureRequest, sink: Sink) -> Iterator[str]:
    """A unique sink channel (and run key) for one test, cleaned up afterwards."""
    name = f"{request.node.name[:40]}-{uuid.uuid4().hex[:8]}".replace("[", "_").replace("]", "_")
    yield name
    sink.reset(name)
    sink.reset(f"api:{name}")


@pytest.fixture
def params(channel: str) -> Callable[..., Dict[str, Any]]:
    """Runtime params for a test: its channel as ``run`` and the workers' view of the sink."""

    def make(**extra: Any) -> Dict[str, Any]:
        return {"run": channel, "sink_url": SINK_INTERNAL_URL, **extra}

    return make


@pytest.fixture
def es_params(channel: str) -> Callable[..., Dict[str, Any]]:
    def make(**extra: Any) -> Dict[str, Any]:
        return {
            "run": channel,
            "sink_url": SINK_INTERNAL_URL,
            "es_url": ES_INTERNAL_URL,
            **extra,
        }

    return make


class Chaos:
    """Kill things on demand. The commands come from the environment, so the same tests
    run against a cluster (``oc delete pod``) or a local stack (``docker kill``)."""

    def _run(self, cmd: str, what: str) -> None:
        if not cmd:
            pytest.skip(f"set E2E_DIST_{what}_CMD to run chaos tests")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, f"{cmd!r} failed: {result.stderr or result.stdout}"

    def kill_worker(self) -> None:
        self._run(KILL_WORKER_CMD, "KILL_WORKER")

    def restart_manager(self) -> None:
        self._run(RESTART_MANAGER_CMD, "RESTART_MANAGER")
        deadline = time.time() + 180
        while time.time() < deadline:  # wait until the replacement serves again
            try:
                if httpx.get(f"{MANAGER_URL}/health", timeout=3).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(2)
        raise AssertionError("manager did not come back within 180s")


@pytest.fixture
def chaos() -> Chaos:
    return Chaos()
