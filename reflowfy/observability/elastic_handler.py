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
import threading
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

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
        timeout = float(os.getenv("ELASTIC_LOG_REQUEST_TIMEOUT", "10"))
        if username and password:
            return Elasticsearch(url, basic_auth=(username, password), request_timeout=timeout)
        return Elasticsearch(url, request_timeout=timeout)

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
            return
        index = f"{self.index}-{datetime.now(timezone.utc):%Y.%m.%d}"
        actions = ({"_index": index, "_source": json.loads(line)} for line in lines)
        try:
            helpers.bulk(self._client, actions, raise_on_error=False)
        except Exception:
            # Never let ES failures crash the shipper thread.
            metrics.logs_dropped_total.inc(len(lines))
