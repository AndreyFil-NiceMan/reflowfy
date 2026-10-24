"""Delivery sink and mock users API for the distributed E2E suite.

Runs next to the Reflowfy deployment (stdlib only, so it needs nothing but Python) and
gives the tests two things a real destination/source cannot:

* a ledger of everything delivered, per channel, so a test can prove each record
  arrived exactly once (``GET /report/{channel}``, ``GET /ids/{channel}``);
* failure injection — a channel can answer 500 for the next N requests, delay, or store
  a batch and then lose the acknowledgement, and the mock API can fail chosen IDs.

Endpoints
    POST /ingest/{channel}        body {"records": [...], "job_index": n, ...}
    PUT  /record/{channel}/{id}   one record per request (api_destination per_record=True);
                                  ``?job=`` tags it, the ledger gets ``id``, and
                                  ``GET /record-stats/{channel}`` reports the requests
                                  open at once per job. Rules: ``record_slow_ms``,
                                  ``record_fail_ids`` (answer 503 while the run's flag is
                                  not "fixed")
    GET  /report/{channel}        counts, duplicates, batch timing
    GET  /ids/{channel}           every received id (repeats kept), in arrival order
    POST /config/{channel}        injection rules, see ``Rules``
    POST /flag/{channel}          {"value": "..."}  — a switch pipelines can poll
    GET  /flag/{channel}
    POST /reset/{channel}         forget a channel (ledger, rules, flag)
    GET  /users/... via /api/{run}/users/{id} and POST /api/{run}/users/batch
    GET  /api/{run}/stats         mock API calls and the most in flight at once, for the
                                  whole run and per ``?job=`` tag the caller sent
    GET  /health, GET /channels, HEAD/OPTIONS on any path (destination health checks)
"""

import json
import os
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

LOCK = threading.Lock()


class Channel:
    """Everything the sink knows about one channel."""

    def __init__(self) -> None:
        self.ids: List[Any] = []
        self.batches: List[Dict[str, Any]] = []
        self.rules: Dict[str, Any] = {}
        self.flag: str = ""
        self.api_calls: Counter = Counter()  # mock API: id -> times requested
        self.in_flight: Counter = Counter()  # mock API: "*" / job tag -> requests open now
        self.peak: Counter = Counter()  # mock API: "*" / job tag -> most open at once


CHANNELS: Dict[str, Channel] = {}


def channel(name: str) -> Channel:
    with LOCK:
        return CHANNELS.setdefault(name, Channel())


def key_of(record: Any) -> Any:
    """The identity of a delivered record: ``id``, else ``data.id``, else the value."""
    if isinstance(record, dict):
        if "id" in record:
            return record["id"]
        data = record.get("data")
        if isinstance(data, dict) and "id" in data:
            return data["id"]
    if isinstance(record, (str, int, float, bool)) or record is None:
        return record
    return json.dumps(record, sort_keys=True)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args: Any) -> None:  # quiet
        pass

    # -- plumbing ---------------------------------------------------------------
    def _body(self) -> Any:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return raw.decode("utf-8", "replace")

    def _send(self, status: int, payload: Any = None) -> None:
        data = b"" if payload is None else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _parts(self) -> List[str]:
        return [p for p in urlparse(self.path).path.split("/") if p]

    # -- verbs ------------------------------------------------------------------
    def do_HEAD(self) -> None:
        self._send(200)

    def do_OPTIONS(self) -> None:
        self._send(200)

    def do_GET(self) -> None:
        parts = self._parts()
        if parts == ["health"]:
            return self._send(200, {"status": "ok"})
        if parts == ["channels"]:
            with LOCK:
                return self._send(200, {"channels": sorted(CHANNELS)})
        if len(parts) == 2 and parts[0] == "report":
            return self._report(parts[1])
        if len(parts) == 2 and parts[0] == "ids":
            ch = channel(parts[1])
            with LOCK:
                return self._send(200, {"ids": list(ch.ids), "batches": list(ch.batches)})
        if len(parts) == 2 and parts[0] == "flag":
            return self._send(200, {"value": channel(parts[1]).flag})
        if len(parts) == 2 and parts[0] == "record-stats":
            return self._record_stats(parts[1])
        if len(parts) == 3 and parts[0] == "api" and parts[2] == "stats":
            return self._api_stats(parts[1])
        if len(parts) == 4 and parts[0] == "api" and parts[2] == "users":
            return self._user(parts[1], parts[3])
        self._send(404, {"error": f"no route {self.path}"})

    def do_DELETE(self) -> None:
        parts = self._parts()
        if len(parts) == 2 and parts[0] == "reset":
            with LOCK:
                CHANNELS.pop(parts[1], None)
            return self._send(200, {"reset": parts[1]})
        self._send(404, {"error": "no route"})

    def do_PUT(self) -> None:
        parts = self._parts()
        body = self._body()
        if len(parts) == 3 and parts[0] == "record":
            return self._record(parts[1], parts[2], body)
        self._send(404, {"error": f"no route {self.path}"})

    def do_POST(self) -> None:
        parts = self._parts()
        body = self._body()
        if len(parts) == 3 and parts[0] == "record":
            return self._record(parts[1], parts[2], body)
        if len(parts) == 2 and parts[0] == "ingest":
            return self._ingest(parts[1], body)
        if len(parts) == 2 and parts[0] == "config":
            channel(parts[1]).rules.update(body or {})
            return self._send(200, {"rules": channel(parts[1]).rules})
        if len(parts) == 2 and parts[0] == "flag":
            channel(parts[1]).flag = str((body or {}).get("value", ""))
            return self._send(200, {"value": channel(parts[1]).flag})
        if len(parts) == 2 and parts[0] == "reset":
            with LOCK:
                CHANNELS.pop(parts[1], None)
            return self._send(200, {"reset": parts[1]})
        if len(parts) == 4 and parts[0] == "api" and parts[2:] == ["users", "batch"]:
            return self._users_batch(parts[1], body)
        self._send(404, {"error": f"no route {self.path}"})

    # -- ingest -----------------------------------------------------------------
    def _ingest(self, name: str, body: Any) -> None:
        ch = channel(name)
        rules = ch.rules
        if rules.get("delay_s"):
            time.sleep(float(rules["delay_s"]))
        with LOCK:
            fail = rules.get("fail_next", 0)
            if fail:
                rules["fail_next"] = fail - 1
        if fail:
            return self._send(int(rules.get("fail_status", 500)), {"error": "injected failure"})
        records = (body or {}).get("records", []) if isinstance(body, dict) else []
        with LOCK:
            for record in records:
                ch.ids.append(key_of(record))
            ch.batches.append(
                {
                    "size": len(records),
                    "at": time.time(),
                    "job_index": (body or {}).get("job_index") if isinstance(body, dict) else None,
                    # whatever else the job sent besides its records (current_ids, flags…)
                    "meta": (
                        {k: v for k, v in body.items() if k != "records"}
                        if isinstance(body, dict)
                        else {}
                    ),
                }
            )
            lost = rules.get("lose_ack_next", 0)
            if lost:
                rules["lose_ack_next"] = lost - 1
        if lost:  # stored, but the client is told it failed: the retry will duplicate
            return self._send(500, {"error": "injected lost acknowledgement"})
        self._send(200, {"accepted": len(records)})

    # -- per-record delivery ----------------------------------------------------
    def _record(self, name: str, rid: str, body: Any) -> None:
        ch = channel(name)
        rules = ch.rules
        job = parse_qs(urlparse(self.path).query).get("job", [""])[0]
        keys = ["rec:*", f"rec:job:{job}"]
        with LOCK:
            for key in keys:
                ch.in_flight[key] += 1
                ch.peak[key] = max(ch.peak[key], ch.in_flight[key])
        try:
            if rules.get("record_slow_ms"):
                time.sleep(float(rules["record_slow_ms"]) / 1000)
            failing = {str(i) for i in rules.get("record_fail_ids", [])}
            if rid in failing and ch.flag != "fixed":
                return self._send(503, {"error": f"injected failure for {rid}"})
            with LOCK:
                # The body is the record (or the part key_field selected): its id must
                # be the one in the URL, or the placeholder was filled from another record.
                body_id = key_of(body)
                ch.ids.append(body_id if str(body_id) == rid else f"mismatch:{rid}:{body_id}")
                ch.batches.append({"size": 1, "at": time.time(), "job_index": job, "meta": {}})
        finally:
            with LOCK:
                for key in keys:
                    ch.in_flight[key] -= 1
        self._send(200, {"accepted": 1})

    def _record_stats(self, name: str) -> None:
        ch = channel(name)
        with LOCK:
            self._send(
                200,
                {
                    "peak_concurrency": ch.peak["rec:*"],
                    "peak_by_job": {
                        k[8:]: v for k, v in ch.peak.items() if k.startswith("rec:job:")
                    },
                },
            )

    def _report(self, name: str) -> None:
        ch = channel(name)
        with LOCK:
            counts = Counter(ch.ids)
            batches = list(ch.batches)
        dups = {str(k): v for k, v in counts.items() if v > 1}
        self._send(
            200,
            {
                "records": len(ch.ids),
                "unique": len(counts),
                "duplicates": dict(list(dups.items())[:200]),
                "duplicate_ids": len(dups),
                "batches": len(batches),
                "first_at": batches[0]["at"] if batches else None,
                "last_at": batches[-1]["at"] if batches else None,
            },
        )

    # -- mock users API ---------------------------------------------------------
    def _api_rules(self, run: str) -> Channel:
        return channel(f"api:{run}")

    def _user_response(self, ch: Channel, run: str, uid: str) -> Optional[Dict[str, Any]]:
        """None means 'respond with the injected status in self._pending_status'."""
        rules = ch.rules
        if rules.get("slow_ms"):
            time.sleep(float(rules["slow_ms"]) / 1000)
        with LOCK:
            ch.api_calls[uid] += 1
            calls = ch.api_calls[uid]
        if uid in {str(i) for i in rules.get("missing_ids", [])}:
            self._pending_status = 404
            return None
        status_by_id = {str(k): int(v) for k, v in rules.get("status_by_id", {}).items()}
        if uid in status_by_id:
            self._pending_status = status_by_id[uid]
            return None
        fail_first = {str(k): int(v) for k, v in rules.get("fail_first", {}).items()}
        if uid in fail_first and calls <= fail_first[uid]:
            self._pending_status = int(rules.get("fail_first_status", 500))
            return None
        return {"id": int(uid) if uid.lstrip("-").isdigit() else uid, "name": f"user-{uid}", "run": run}

    def _user(self, run: str, uid: str) -> None:
        ch = self._api_rules(run)
        job = parse_qs(urlparse(self.path).query).get("job", [None])[0]
        keys = ["*"] + ([f"job:{job}"] if job is not None else [])
        with LOCK:
            for key in keys:
                ch.in_flight[key] += 1
                ch.peak[key] = max(ch.peak[key], ch.in_flight[key])
        try:
            user = self._user_response(ch, run, uid)
        finally:
            with LOCK:
                for key in keys:
                    ch.in_flight[key] -= 1
        if user is None:
            return self._send(self._pending_status, {"error": "injected"})
        self._send(200, user)

    def _api_stats(self, run: str) -> None:
        ch = self._api_rules(run)
        with LOCK:
            self._send(
                200,
                {
                    "calls": sum(ch.api_calls.values()),
                    "peak_concurrency": ch.peak["*"],
                    "peak_by_job": {k[4:]: v for k, v in ch.peak.items() if k.startswith("job:")},
                },
            )

    def _users_batch(self, run: str, body: Any) -> None:
        ch = self._api_rules(run)
        users = []
        for uid in (body or {}).get("ids", []):
            user = self._user_response(ch, run, str(uid))
            if user is None:
                if self._pending_status >= 500 or self._pending_status == 429:
                    return self._send(self._pending_status, {"error": f"injected for {uid}"})
                continue  # 404: missing from the batch
            users.append(user)
        self._send(200, {"users": users})


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    print(f"e2e sink listening on :{port}", flush=True)
    Server(("0.0.0.0", port), Handler).serve_forever()
