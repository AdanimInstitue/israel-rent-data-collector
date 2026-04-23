from __future__ import annotations

from itertools import count

import requests
from requests import HTTPError

from rent_collector.utils import http_client
from rent_collector.utils.http_client import (
    RateLimitedSession,
    _is_retryable_error,
    _maybe_raise_retryable_status,
    get_client,
)


class _Response:
    def __init__(
        self, *, json_data=None, content: bytes = b"payload", status_code: int = 200
    ) -> None:
        self._json_data = json_data if json_data is not None else {"ok": True}
        self.content = content
        self.status_code = status_code
        self.status_checked = False

    def json(self):
        return self._json_data

    def raise_for_status(self) -> None:
        self.status_checked = True


def test_rate_limited_session_get_post_and_helpers(monkeypatch) -> None:
    session = RateLimitedSession(delay=1.0, timeout=5.0, user_agent="test-agent")
    get_calls: list[tuple[str, object]] = []
    post_calls: list[tuple[str, object, object]] = []
    sleeps: list[float] = []
    monotonic_values = count()

    def fake_get(url, **kwargs):
        get_calls.append((url, kwargs.get("params")))
        return _Response(json_data={"url": url})

    def fake_post(url, **kwargs):
        post_calls.append((url, kwargs.get("json"), kwargs.get("data")))
        return _Response()

    monkeypatch.setattr(session._session, "get", fake_get)
    monkeypatch.setattr(session._session, "post", fake_post)
    monkeypatch.setattr("time.monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr("time.sleep", sleeps.append)

    response = session.get("https://example.com/data", params={"a": 1})
    assert response.json() == {"url": "https://example.com/data"}

    json_payload = session.get_json("https://example.com/json")
    bytes_payload = session.get_bytes("https://example.com/bytes")
    post_response = session.post("https://example.com/post", json={"x": 1})

    assert get_calls[0] == ("https://example.com/data", {"a": 1})
    assert json_payload == {"url": "https://example.com/json"}
    assert bytes_payload == b"payload"
    assert post_calls == [("https://example.com/post", {"x": 1}, None)]
    assert sleeps == []
    assert post_response.status_checked is True

    closed = {"value": False}
    monkeypatch.setattr(session._session, "close", lambda: closed.__setitem__("value", True))
    with session as same_session:
        assert same_session is session
    assert closed["value"] is True


def test_rate_limited_session_post_can_skip_raise_for_status(monkeypatch) -> None:
    session = RateLimitedSession(delay=0.0, timeout=5.0, user_agent="test-agent")
    response = _Response()

    monkeypatch.setattr(session._session, "post", lambda *_args, **_kwargs: response)

    returned = session.post("https://example.com/post", data={"x": 1}, raise_for_status=False)

    assert returned is response
    assert response.status_checked is False


def test_rate_limited_session_retries_http_5xx_even_when_raise_for_status_is_false(
    monkeypatch,
) -> None:
    session = RateLimitedSession(delay=0.0, timeout=5.0, user_agent="test-agent")
    attempts = {"count": 0}

    class _RetryResponse(_Response):
        def raise_for_status(self) -> None:
            self.status_checked = True
            if self.status_code >= 500:
                error = HTTPError("server error")
                error.response = self
                raise error

    responses = [
        _RetryResponse(status_code=502),
        _RetryResponse(json_data={"ok": True}, status_code=200),
    ]

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        return responses.pop(0)

    monkeypatch.setattr(session._session, "get", fake_get)

    result = session.get("https://example.com/retry", raise_for_status=False)

    assert result.status_code == 200
    assert attempts["count"] == 2


def test_rate_limited_session_does_not_retry_http_4xx(monkeypatch) -> None:
    session = RateLimitedSession(delay=0.0, timeout=5.0, user_agent="test-agent")
    attempts = {"count": 0}

    class _ClientErrorResponse(_Response):
        def raise_for_status(self) -> None:
            self.status_checked = True
            if self.status_code >= 400:
                error = HTTPError("client error")
                error.response = self
                raise error

    response = _ClientErrorResponse(status_code=404)

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        return response

    monkeypatch.setattr(session._session, "get", fake_get)

    try:
        session.get("https://example.com/missing")
    except HTTPError as exc:
        assert exc.response is response
    else:
        raise AssertionError("Expected HTTPError for 404 response")

    assert attempts["count"] == 1


def test_rate_limited_session_throttle_sleeps_when_requests_are_too_close(monkeypatch) -> None:
    session = RateLimitedSession(delay=1.0, timeout=5.0, user_agent="test-agent")
    sleeps: list[float] = []
    monotonic_values = iter([0.2, 0.3])

    monkeypatch.setattr("time.monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr("time.sleep", sleeps.append)
    session._last_request_time["example.com"] = 0.0

    session._throttle("example.com")

    assert sleeps == [0.8]


def test_retryability_helpers_cover_timeout_http_error_and_generic_cases() -> None:
    timeout = requests.Timeout("slow")
    generic = RuntimeError("boom")
    server_error = HTTPError("server")
    server_error.response = _Response(status_code=503)
    response_less_error = HTTPError("response missing")

    assert _is_retryable_error(timeout) is True
    assert _is_retryable_error(server_error) is True
    assert _is_retryable_error(response_less_error) is False
    assert _is_retryable_error(generic) is False


def test_maybe_raise_retryable_status_attaches_response_when_missing() -> None:
    class _ServerErrorResponse(_Response):
        def raise_for_status(self) -> None:
            raise HTTPError("server error")

    response = _ServerErrorResponse(status_code=502)

    try:
        _maybe_raise_retryable_status(response)
    except HTTPError as exc:
        assert exc.response is response
    else:
        raise AssertionError("Expected HTTPError for retryable 5xx status")


def test_get_client_returns_singleton() -> None:
    http_client._default_client = None
    first = get_client()
    second = get_client()
    try:
        assert first is second
    finally:
        first.close()
        http_client._default_client = None
