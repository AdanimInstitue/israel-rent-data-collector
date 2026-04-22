"""
Polite, rate-limited HTTP client with automatic retries.

Wraps `requests` with:
  - Configurable per-host delay (default 1.2 s) to avoid hammering government servers
  - Exponential backoff retries on transient errors and HTTP 5xx responses
  - Consistent User-Agent header
  - Request logging via `rich`
"""

from __future__ import annotations

import time
from typing import Any

import requests
from requests import Response, Session
from rich.console import Console
from tenacity import (
    retry,
    retry_if_exception,
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


def _is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.Timeout | requests.ConnectionError):
        return True
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        return response is not None and response.status_code >= 500
    return False


def _maybe_raise_retryable_status(resp: Response) -> None:
    """Raise for HTTP 5xx so tenacity can retry transient server-side failures."""
    if 500 <= resp.status_code < 600:
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is None:
                exc.response = resp
            raise


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
        self._last_request_time: dict[str, float] = {}
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
        now = time.monotonic()
        last_request_time = self._last_request_time.get(host)
        if last_request_time is not None:
            elapsed = now - last_request_time
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
        self._last_request_time[host] = time.monotonic()

    @retry(
        retry=retry_if_exception(_is_retryable_error),
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
        _maybe_raise_retryable_status(resp)
        if raise_for_status:
            resp.raise_for_status()
        return resp

    @retry(
        retry=retry_if_exception(_is_retryable_error),
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
        _maybe_raise_retryable_status(resp)
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
        return bytes(resp.content)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> RateLimitedSession:
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
