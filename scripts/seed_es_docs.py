"""Index the documents the distributed suite's Elasticsearch tests read.

Index ``docs`` with ``seq = 1..N`` (default 20000), as tests/e2e_distributed/README.md
asks. Stdlib only, so it runs before any project dependency is installed.

    python scripts/seed_es_docs.py [--url http://localhost:9201] [--count 20000]
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

BATCH = 2000


def _request(method: str, url: str, body: bytes = b"", content_type: str = "application/json") -> dict:
    req = urllib.request.Request(url, data=body or None, method=method)
    req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read() or b"{}")


def _wait_for(url: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while True:
        try:
            health = _request("GET", f"{url}/_cluster/health?wait_for_status=yellow&timeout=5s")
            if health.get("status") in ("yellow", "green"):
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        if time.monotonic() > deadline:
            sys.exit(f"Elasticsearch at {url} is not up after {seconds:.0f}s")
        time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:9201")
    parser.add_argument("--index", default="docs")
    parser.add_argument("--count", type=int, default=20000)
    args = parser.parse_args()
    url = args.url.rstrip("/")

    _wait_for(url, 180)
    try:
        _request("DELETE", f"{url}/{args.index}")
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    _request(
        "PUT",
        f"{url}/{args.index}",
        json.dumps({"mappings": {"properties": {"seq": {"type": "long"}}}}).encode(),
    )
    for start in range(1, args.count + 1, BATCH):
        lines = []
        for seq in range(start, min(start + BATCH, args.count + 1)):
            lines.append(json.dumps({"index": {"_index": args.index, "_id": str(seq)}}))
            lines.append(json.dumps({"seq": seq}))
        result = _request(
            "POST", f"{url}/_bulk", ("\n".join(lines) + "\n").encode(), "application/x-ndjson"
        )
        if result.get("errors"):
            sys.exit(f"bulk indexing reported errors at seq {start}")
    _request("POST", f"{url}/{args.index}/_refresh")
    count = _request("GET", f"{url}/{args.index}/_count")["count"]
    if count != args.count:
        sys.exit(f"expected {args.count} documents in {args.index}, found {count}")
    print(f"indexed {count} documents into {args.index}")


if __name__ == "__main__":
    main()
