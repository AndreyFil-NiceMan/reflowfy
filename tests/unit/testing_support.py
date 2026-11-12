"""Shared helpers for the ``reflowfy.testing`` tests: a local HTTP server and an
isolated pipeline registry."""

import gzip
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List

from reflowfy import pipeline_registry


class LocalServer:
    """A real HTTP server on localhost that counts what it is asked."""

    def __init__(self) -> None:
        self.hits: List[str] = []
        self.bodies: List[bytes] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def _answer(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                outer.bodies.append(self.rfile.read(length) if length else b"")
                outer.hits.append(f"{self.command} {self.path}")
                path = self.path.split("?")[0]
                if path.startswith("/missing"):
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if path.startswith("/redirect"):
                    self.send_response(302)
                    self.send_header("Location", "/json/redirected")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                headers: Dict[str, str] = {"Set-Cookie": "session=secret"}
                if path.startswith("/text"):
                    body, headers["Content-Type"] = b"plain words", "text/plain"
                elif path.startswith("/binary"):
                    body, headers["Content-Type"] = b"\xff\xfe\x00\x01", "application/octet-stream"
                elif path.startswith("/badjson"):
                    body, headers["Content-Type"] = b"{not json", "application/json"
                elif path.startswith("/null"):
                    body, headers["Content-Type"] = b"null", "application/json"
                elif path.startswith("/gzip"):
                    body = gzip.compress(json.dumps({"zipped": True}).encode())
                    headers["Content-Type"] = "application/json"
                    headers["Content-Encoding"] = "gzip"
                elif path.startswith("/big"):
                    body, headers["Content-Type"] = b"x" * 4096, "text/plain"
                elif path.startswith("/empty"):
                    body = b""
                else:
                    payload = {"path": path, "email": "a@b.c", "method": self.command}
                    body, headers["Content-Type"] = json.dumps(payload).encode(), "application/json"
                self.send_response(200)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _answer

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        self._thread = threading.Thread(target=lambda: self._server.serve_forever(poll_interval=0.01), daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@contextmanager
def local_server() -> Iterator[LocalServer]:
    server = LocalServer()
    try:
        yield server
    finally:
        server.close()


@contextmanager
def isolated_registry() -> Iterator[None]:
    """Pipelines a test defines are gone when it ends."""
    saved = dict(pipeline_registry._pipelines)
    saved_failures = dict(pipeline_registry._failures)
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)
        pipeline_registry._failures.clear()
        pipeline_registry._failures.update(saved_failures)
