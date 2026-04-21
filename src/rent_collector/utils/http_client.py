"""
Polite, rate-limited HTTP client with automatic retries.

Wraps `requests` with:
  - Configurable per-host delay (default 1.2 s) to avoid hammering government servers
  - Exponential backoff retries on 5xx / transient errors
  - Consistent User-Agent header
  - Request logging via `rich`
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

import requests
from requests import Response, Session
from rich.console import Console
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from rent_collector.config import (
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    USER_AGENT,
)

console = Console(stderr=True)


class RateLimitedSession:
    """
    A thin wrapper around `requests.Session` that enforces a minimum delay
    between consecutive requests to the same host.
    """

    def __init__(
        self,
        delay: float = REQUEST_DELAY_SECONDS,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
        user_agent: str = USER_AGENT,
    ) -> None:
        self._delay = delay
        self._timeout = timeout
        self._last_request_time: dict[str, float] = defaultdict(float)
        self._session = Session()
        self._session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "he,en;q=0.9",
            }
        )

    def _throttle(self, host: str) -> None:
        """Sleep if needed to respect the per-host rate limit."""
        elapsed = time.monotonic() - self._last_request_time[host]
        if elapsed < self._delay:
            sleep_for = self._delay - elapsed
            time.sleep(sleep_for)
        self._last_request_time[host] = time.monotonic()

    @retry(
        retry=retry_if_exception_type(
            (requests.Timeout, requests.ConnectionError, requests.HTTPError)
        ),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> Response:
        from urllib.parse import urlparse

        host = urlparse(url).netloc
        self._throttle(host)
        console.log(f"[dim]GET[/dim] {url}" + (f" {params}" if params else ""))
        resp = self._session.get(
            url,
            params=params,
            headers=headers,
            timeout=self._timeout,
        )
        if raise_for_status:
            resp.raise_for_status()
        return resp

    @retry(
        retry=retry_if_exception_type(
            (requests.Timeout, requests.ConnectionError, requests.HTTPError)
        ),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    def post(
        self,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> Response:
        from urllib.parse import urlparse

        host = urlparse(url).netloc
        self._throttle(host)
        console.log(f"[dim]POST[/dim] {url}")
        resp = self._session.post(
            url,
            json=json,
            data=data,
            headers=headers,
            timeout=self._timeout,
        )
        if raise_for_status:
            resp.raise_for_status()
        return resp

    def get_json(self, url: str, **kwargs: Any) -> Any:
        """GET and parse JSON response."""
        resp = self.get(url, **kwargs)
        return resp.json()

    def get_bytes(self, url: str, **kwargs: Any) -> bytes:
        """GET and return raw bytes (for PDF/Excel downloads)."""
        resp = self.get(url, raise_for_status=True, **kwargs)
        return resp.content

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "RateLimitedSession":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# Module-level singleton for convenience
_default_client: RateLimitedSession | None = None


def get_client() -> RateLimitedSession:
    """Return the module-level shared HTTP client (created on first call)."""
    global _default_client
    if _default_client is None:
        _default_client = RateLimitedSession()
    return _default_client
