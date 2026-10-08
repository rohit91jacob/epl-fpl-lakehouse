"""HTTP behaviour is tested against a real local server, so urllib3's retry logic runs."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar

import pytest

from epl_lakehouse.ingestion.client import (
    ContractError,
    FileTransport,
    FplApiError,
    FplClient,
    HttpTransport,
    RateLimiter,
)

BOOTSTRAP = {"events": [], "teams": [], "elements": [], "element_types": []}


class FakeFpl(BaseHTTPRequestHandler):
    """Routes: /flaky/* fails twice with 503 then succeeds; /broken/* returns garbage."""

    hits: ClassVar[dict[str, int]] = {}

    def do_GET(self) -> None:
        FakeFpl.hits[self.path] = FakeFpl.hits.get(self.path, 0) + 1
        if self.path.startswith("/flaky/") and FakeFpl.hits[self.path] <= 2:
            self._send(503, b"busy")
        elif self.path.startswith("/missing/"):
            self._send(404, b"not found")
        elif self.path.startswith("/broken/"):
            self._send(200, b"<html>maintenance</html>")
        elif self.path.endswith("bootstrap-static/"):
            self._send(200, json.dumps(BOOTSTRAP).encode())
        elif self.path.endswith("fixtures/"):
            self._send(200, b"[]")
        else:
            self._send(200, b'{"unexpected": true}')

    def _send(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:  # keep test output clean
        pass


@pytest.fixture
def server() -> Iterator[str]:
    FakeFpl.hits = {}
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), FakeFpl)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def client(base_url: str, retries: int = 3) -> FplClient:
    transport = HttpTransport(
        base_url,
        timeout=5,
        max_retries=retries,
        backoff_seconds=0.01,
        user_agent="tests",
        limiter=RateLimiter(0),
    )
    return FplClient(transport)


def test_successful_fetch_parses_and_records_source(server: str) -> None:
    result = client(server + "/api").bootstrap_static()
    assert result.data == BOOTSTRAP
    assert result.source_url == server + "/api/bootstrap-static/"
    assert result.fetched_at.tzinfo is not None


def test_transient_5xx_is_retried(server: str) -> None:
    result = client(server + "/flaky").fixtures()
    assert result.data == []
    assert FakeFpl.hits["/flaky/fixtures/"] == 3


def test_retries_are_bounded(server: str) -> None:
    with pytest.raises(FplApiError, match="HTTP 503"):
        client(server + "/flaky", retries=1).fixtures()


def test_client_errors_are_not_retried(server: str) -> None:
    with pytest.raises(FplApiError, match="HTTP 404"):
        client(server + "/missing").fixtures()
    assert FakeFpl.hits["/missing/fixtures/"] == 1


def test_non_json_is_a_contract_error(server: str) -> None:
    with pytest.raises(ContractError, match="did not return JSON"):
        client(server + "/broken").fixtures()


def test_missing_required_keys_is_a_contract_error(server: str) -> None:
    with pytest.raises(ContractError, match="missing required keys"):
        client(server + "/api").event_live(1)


def test_file_transport_maps_api_paths(tmp_path: Path) -> None:
    (tmp_path / "event" / "3").mkdir(parents=True)
    (tmp_path / "event" / "3" / "live.json").write_text('{"elements": []}')
    result = FplClient(FileTransport(tmp_path)).event_live(3)
    assert result.resource_id == "3"
    assert result.source_url.endswith("/event/3/live.json")
    with pytest.raises(FplApiError, match="no captured response"):
        FplClient(FileTransport(tmp_path)).fixtures()


def test_rate_limiter_spaces_calls() -> None:
    limiter = RateLimiter(50)
    start = time.monotonic()
    for _ in range(6):
        limiter.acquire()
    assert time.monotonic() - start >= 0.09  # 5 intervals of 20 ms
