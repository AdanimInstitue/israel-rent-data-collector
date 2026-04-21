from __future__ import annotations

from itertools import count

from rent_collector.utils.http_client import RateLimitedSession


class _Response:
    def __init__(self, *, json_data=None, content: bytes = b"payload") -> None:
        self._json_data = json_data if json_data is not None else {"ok": True}
        self.content = content
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
