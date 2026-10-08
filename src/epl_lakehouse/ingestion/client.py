"""Fantasy Premier League API client with retries, rate limiting and response contracts."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from urllib.request import url2pathname

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from epl_lakehouse.config import Settings

log = logging.getLogger(__name__)

# Top-level keys each endpoint must return. A missing key means the upstream contract
# changed and we fail fast instead of silently landing unusable data.
REQUIRED_KEYS: dict[str, frozenset[str]] = {
    "bootstrap-static": frozenset({"events", "teams", "elements", "element_types"}),
    "event-live": frozenset({"elements"}),
    "element-summary": frozenset({"history", "fixtures", "history_past"}),
}


class FplApiError(RuntimeError):
    """The API could not be reached or returned an unusable response."""


class ContractError(FplApiError):
    """The response parsed but does not have the shape the pipeline depends on."""


class Transport(Protocol):
    def get(self, path: str) -> tuple[bytes, str]:
        """Return ``(body, source_url)`` for an API path such as ``"fixtures/"``."""
        ...


class RateLimiter:
    """Thread-safe limiter that spaces calls at least ``1/rate`` seconds apart."""

    def __init__(self, max_per_second: float) -> None:
        self._interval = 1.0 / max_per_second if max_per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self._interval
        if wait > 0:
            time.sleep(wait)


class HttpTransport:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float,
        max_retries: int,
        backoff_seconds: float,
        user_agent: str,
        limiter: RateLimiter,
        pool_size: int = 8,
    ) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._timeout = timeout
        self._limiter = limiter
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_seconds,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def get(self, path: str) -> tuple[bytes, str]:
        url = self._base_url + path.lstrip("/")
        self._limiter.acquire()
        try:
            response = self._session.get(url, timeout=self._timeout)
        except requests.RequestException as exc:
            raise FplApiError(f"GET {url} failed: {exc}") from exc
        if response.status_code != 200:
            raise FplApiError(f"GET {url} returned HTTP {response.status_code}")
        return response.content, url


class FileTransport:
    """Serves API paths from a directory, e.g. ``event/5/live/`` -> ``<root>/event/5/live.json``.

    Used for offline runs, CI smoke tests and replaying captured responses.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def get(self, path: str) -> tuple[bytes, str]:
        file = self._root / (path.strip("/") + ".json")
        try:
            return file.read_bytes(), file.as_uri()
        except FileNotFoundError as exc:
            raise FplApiError(f"no captured response for {path!r} at {file}") from exc


def transport_from_settings(settings: Settings) -> Transport:
    parsed = urlparse(settings.fpl_base_url)
    if parsed.scheme == "file":
        return FileTransport(Path(url2pathname(parsed.path)))
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported EPL_FPL_BASE_URL scheme: {settings.fpl_base_url}")
    return HttpTransport(
        settings.fpl_base_url,
        timeout=settings.http_timeout_seconds,
        max_retries=settings.http_max_retries,
        backoff_seconds=settings.http_backoff_seconds,
        user_agent=settings.user_agent,
        limiter=RateLimiter(settings.max_requests_per_second),
        pool_size=max(settings.max_workers, 1),
    )


@dataclass(frozen=True)
class FetchResult:
    endpoint: str
    resource_id: str | None
    source_url: str
    fetched_at: datetime
    content: bytes
    data: Any


def _utc_now() -> datetime:
    return datetime.now(UTC)


class FplClient:
    def __init__(self, transport: Transport, clock: Callable[[], datetime] = _utc_now) -> None:
        self._transport = transport
        self._clock = clock

    def _fetch(self, endpoint: str, path: str, resource_id: str | None = None) -> FetchResult:
        content, url = self._transport.get(path)
        fetched_at = self._clock()
        try:
            data = json.loads(content)
        except ValueError as exc:
            raise ContractError(f"{url} did not return JSON") from exc
        _check_contract(endpoint, data, url)
        log.debug("fetched", extra={"endpoint": endpoint, "url": url, "bytes": len(content)})
        return FetchResult(endpoint, resource_id, url, fetched_at, content, data)

    def bootstrap_static(self) -> FetchResult:
        return self._fetch("bootstrap-static", "bootstrap-static/")

    def fixtures(self) -> FetchResult:
        return self._fetch("fixtures", "fixtures/")

    def event_live(self, gameweek: int) -> FetchResult:
        return self._fetch("event-live", f"event/{gameweek}/live/", str(gameweek))

    def element_summary(self, player_id: int) -> FetchResult:
        return self._fetch("element-summary", f"element-summary/{player_id}/", str(player_id))


def _check_contract(endpoint: str, data: Any, url: str) -> None:
    if endpoint == "fixtures":
        if not isinstance(data, list):
            raise ContractError(f"{url}: expected a JSON array of fixtures")
        return
    if not isinstance(data, dict):
        raise ContractError(f"{url}: expected a JSON object")
    missing = REQUIRED_KEYS[endpoint] - data.keys()
    if missing:
        raise ContractError(f"{url}: missing required keys {sorted(missing)}")
