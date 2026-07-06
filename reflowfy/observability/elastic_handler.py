"""A logging.Handler that bulk-ships records to Elasticsearch off the hot path.

Design constraint (500k jobs/hr): never block the pipeline. Records go onto a
bounded in-memory queue; a daemon thread bulk-indexes them. When the queue is
full we drop the OLDEST record and increment a counter — a slow/hiccuping ES
must never stall job processing.
# ponytail: in-memory drop-oldest; upgrade path = local disk spool if drops hurt.
"""

import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, helpers

from reflowfy.observability import metrics


class ElasticLogHandler(logging.Handler):
    """Bounded, batching, non-blocking Elasticsearch log shipper."""

    def __init__(
        self,
        service_name: str = "reflowfy",
        client: Optional[Any] = None,
        index: Optional[str] = None,
        flush_docs: Optional[int] = None,
        flush_seconds: Optional[float] = None,
        queue_max: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.service_name = service_name
        self.index = index or os.getenv("ELASTIC_LOG_INDEX", "reflowfy-logs")
        self.flush_docs = flush_docs or int(os.getenv("ELASTIC_LOG_FLUSH_DOCS", "2000"))
        self.flush_seconds = flush_seconds or float(os.getenv("ELASTIC_LOG_FLUSH_SECONDS", "1"))
        qmax = queue_max or int(os.getenv("ELASTIC_LOG_QUEUE_MAX", "50000"))
        self._q: "queue.Queue[str]" = queue.Queue(maxsize=qmax)
        self._client = client if client is not None else self._build_client()
        self._stop = threading.Event()
        self._paused = False  # test hook
        self._last_warn = 0.0
        self._thread = threading.Thread(target=self._run, name="es-log-shipper", daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            return
        try:
            self._q.put_nowait(line)
        except queue.Full:
            # Drop oldest to make room; count the loss.
            try:
                self._q.get_nowait()
                metrics.logs_dropped_total.inc()
                self._q.put_nowait(line)
            except queue.Empty:
                metrics.logs_dropped_total.inc()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        super().close()

    # --- internals ---
    def _build_client(self) -> Optional[Elasticsearch]:
        url = os.getenv("ELASTIC_LOG_URL")
        if not url:
            return None
        username = os.getenv("ELASTIC_LOG_USERNAME") or None
        password = os.getenv("ELASTIC_LOG_PASSWORD") or None
        # Bounded timeout so a slow/hung ES can't stall the shipper thread
        # indefinitely; it fails fast, drops the batch, and keeps draining.
        kwargs: Dict[str, Any] = {
            "request_timeout": float(os.getenv("ELASTIC_LOG_REQUEST_TIMEOUT", "10"))
        }
        if username and password:
            kwargs["basic_auth"] = (username, password)
        # TLS verification is OFF by default so it works out of the box with
        # ES 8's self-signed cert. Set ELASTIC_LOG_VERIFY_CERTS=true to enforce
        # it (optionally with ELASTIC_LOG_CA_CERTS pointing at a CA bundle).
        ca_certs = os.getenv("ELASTIC_LOG_CA_CERTS")
        if ca_certs:
            kwargs["ca_certs"] = ca_certs
        if os.getenv("ELASTIC_LOG_VERIFY_CERTS", "false").lower() != "true":
            kwargs["verify_certs"] = False
            kwargs["ssl_show_warn"] = False
        return Elasticsearch(url, **kwargs)

    def _run(self) -> None:
        batch: List[str] = []
        last = time.monotonic()
        while not self._stop.is_set():
            if self._paused:
                time.sleep(0.05)
                continue
            timeout = max(0.01, self.flush_seconds - (time.monotonic() - last))
            try:
                batch.append(self._q.get(timeout=timeout))
            except queue.Empty:
                pass
            due = len(batch) >= self.flush_docs or (time.monotonic() - last) >= self.flush_seconds
            if batch and due:
                self._flush(batch)
                batch = []
                last = time.monotonic()
        if batch:
            self._flush(batch)

    def _flush(self, lines: List[str]) -> None:
        if self._client is None:
            metrics.logs_dropped_total.inc(len(lines))
            self._warn_once("no Elasticsearch client (ELASTIC_LOG_URL unset); dropping logs")
            return
        index = f"{self.index}-{datetime.now(timezone.utc):%Y.%m.%d}"
        actions = ({"_index": index, "_source": json.loads(line)} for line in lines)
        try:
            # raise_on_error=False so a bad doc doesn't crash the thread, but we
            # MUST inspect `errors` — ES can accept the request yet reject every
            # doc (e.g. data-stream template conflict, blocked auto-create).
            _success, errors = helpers.bulk(
                self._client, actions, raise_on_error=False, stats_only=False
            )
            error_list = errors if isinstance(errors, list) else []
            if error_list:
                metrics.logs_dropped_total.inc(len(error_list))
                self._warn_once(f"Elasticsearch rejected log docs; first error: {error_list[0]}")
        except Exception as e:
            # Transport/connection failure — never let it crash the shipper.
            metrics.logs_dropped_total.inc(len(lines))
            self._warn_once(f"Elasticsearch log shipping failed: {e}")

    def _warn_once(self, msg: str) -> None:
        """Print a shipping problem to stderr at most once per minute (bypasses
        the logging system to avoid a log-about-logging feedback loop)."""
        now = time.monotonic()
        if now - self._last_warn >= 60:
            self._last_warn = now
            print(f"[reflowfy.elastic_handler] {msg}", file=sys.stderr, flush=True)
