"""Clients and assertions shared by the distributed E2E tests."""

import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx

TERMINAL = ("completed", "failed")


def _get(http: httpx.Client, path: str, **kwargs: Any) -> httpx.Response:
    """GET, retried a few times on a dropped connection.

    The tests usually reach the deployment through ``oc port-forward``, which resets
    connections now and then — and the chaos tests kill pods on purpose. Only reads are
    retried: repeating a POST could start a run twice.
    """
    for attempt in range(6):
        try:
            return http.get(path, **kwargs)
        except httpx.TransportError:
            if attempt == 5:
                raise
            time.sleep(1.0 + attempt)
    raise AssertionError("unreachable")


class Manager:
    """A thin client for the ReflowManager REST API."""

    def __init__(self, base_url: str):
        self.http = httpx.Client(base_url=base_url, timeout=60.0)

    def run(self, pipeline: str, params: Dict[str, Any], **options: Any) -> str:
        """POST /run and return the execution id (raises unless the manager accepts it)."""
        response = self.http.post(
            "/run", json={"pipeline_name": pipeline, "runtime_params": params, **options}
        )
        assert response.status_code == 202, f"/run -> {response.status_code}: {response.text}"
        return str(response.json()["execution_id"])

    def try_run(self, pipeline: str, params: Dict[str, Any], **options: Any) -> httpx.Response:
        return self.http.post(
            "/run", json={"pipeline_name": pipeline, "runtime_params": params, **options}
        )

    def stats(self, execution_id: str) -> Dict[str, Any]:
        response = _get(self.http, f"/executions/{execution_id}/stats")
        assert response.status_code == 200, f"stats -> {response.status_code}: {response.text}"
        return dict(response.json())

    def jobs(self, execution_id: str) -> List[Dict[str, Any]]:
        response = _get(self.http, f"/executions/{execution_id}/jobs", params={"limit": 100000})
        assert response.status_code == 200, response.text
        body = response.json()
        return list(body["jobs"] if isinstance(body, dict) else body)

    def errors(self, execution_id: str) -> Any:
        return _get(self.http, f"/executions/{execution_id}/errors").json()

    def post(self, path: str, **kw: Any) -> httpx.Response:
        return self.http.post(path, **kw)

    def wait(
        self,
        execution_id: str,
        states: Sequence[str] = TERMINAL,
        timeout: float = 300.0,
        poll: float = 1.0,
    ) -> Dict[str, Any]:
        """Wait until the execution is in one of ``states``; fail with its stats on timeout."""
        deadline = time.time() + timeout
        stats: Dict[str, Any] = {}
        while time.time() < deadline:
            stats = self.stats(execution_id)
            if stats["state"] in states:
                return stats
            time.sleep(poll)
        raise AssertionError(f"execution {execution_id} not in {states} after {timeout}s: {stats}")

    def wait_jobs(self, execution_id: str, at_least: int, timeout: float = 120.0) -> Dict[str, Any]:
        """Wait until at least ``at_least`` jobs have finished (any terminal job state)."""
        deadline = time.time() + timeout
        stats: Dict[str, Any] = {}
        while time.time() < deadline:
            stats = self.stats(execution_id)
            if stats["jobs_completed"] + stats["jobs_failed"] >= at_least:
                return stats
            time.sleep(0.5)
        raise AssertionError(f"fewer than {at_least} jobs finished after {timeout}s: {stats}")


class Sink:
    """A client for the delivery sink (see sink/server.py)."""

    def __init__(self, base_url: str):
        self.http = httpx.Client(base_url=base_url, timeout=60.0)

    def ids(self, channel: str) -> List[Any]:
        return list(_get(self.http, f"/ids/{channel}").json()["ids"])

    def batches(self, channel: str) -> List[Dict[str, Any]]:
        return list(_get(self.http, f"/ids/{channel}").json()["batches"])

    def report(self, channel: str) -> Dict[str, Any]:
        return dict(_get(self.http, f"/report/{channel}").json())

    def rules(self, channel: str, **rules: Any) -> None:
        self.http.post(f"/config/{channel}", json=rules).raise_for_status()

    def flag(self, channel: str, value: str) -> None:
        self.http.post(f"/flag/{channel}", json={"value": value}).raise_for_status()

    def reset(self, channel: str) -> None:
        self.http.delete(f"/reset/{channel}")

    def api_rules(self, channel: str, **rules: Any) -> None:
        """Injection rules for the mock users API of a run (``slow_ms``, ``status_by_id``…)."""
        self.rules(f"api:{channel}", **rules)

    def api_stats(self, channel: str) -> Dict[str, Any]:
        """Mock users API calls and peak concurrency: whole run and ``peak_by_job``."""
        return dict(_get(self.http, f"/api/{channel}/stats").json())


def assert_exactly_once(sink: Sink, channel: str, expected: Iterable[Any]) -> None:
    """Every expected id arrived, once, and nothing else arrived."""
    want = list(expected)
    got = sink.ids(channel)
    counts: Dict[Any, int] = {}
    for item in got:
        counts[item] = counts.get(item, 0) + 1
    missing = [i for i in want if i not in counts]
    extra = [i for i in counts if i not in set(want)]
    dups = {i: n for i, n in counts.items() if n > 1}
    assert not (missing or extra or dups), (
        f"channel {channel}: expected {len(want)} ids once each, got {len(got)} "
        f"({len(counts)} unique); missing={missing[:10]}{'…' if len(missing) > 10 else ''} "
        f"({len(missing)}), unexpected={extra[:10]} ({len(extra)}), "
        f"duplicated={dict(list(dups.items())[:10])} ({len(dups)})"
    )


def job_states(manager: Manager, execution_id: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in manager.jobs(execution_id):
        counts[item["state"]] = counts.get(item["state"], 0) + 1
    return counts


def first(items: Optional[List[Any]]) -> Any:
    return items[0] if items else None
