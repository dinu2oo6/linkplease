import hashlib
import hmac
import json
import os
import uuid

import pytest

os.environ.setdefault("PSEUDOGRAM_API_KEY", "test-secret-key")
os.environ.setdefault("REQUIRE_SIGNATURE", "true")

from app import config, db  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Every test gets its own database file."""
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "test.db"))
    db.reset_for_tests()
    db.connect()
    yield
    db.reset_for_tests()


@pytest.fixture
def api_key():
    return config.API_KEY


def sign(raw: bytes, secret: str | None = None) -> str:
    secret = secret or config.API_KEY
    return "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def make_event(text="PRICE please", user_id="usr_1", event_id=None,
               comment_id=None, event_type="comment.created") -> dict:
    data = {"comment_id": comment_id or f"cmt_{uuid.uuid4().hex[:8]}"}
    if event_type == "comment.created":
        data.update({
            "post_id": "post_1", "text": text, "created_at": "2026-08-15T00:00:00Z",
            "from": {"user_id": user_id, "username": f"name_{user_id}"},
        })
    return {
        "event_id": event_id or f"evt_{uuid.uuid4().hex[:12]}",
        "event_type": event_type,
        "sent_at": "2026-08-15T00:00:00Z",
        "data": data,
    }


def raw_of(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode()


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None, text=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text if text is not None else json.dumps(self._payload)

    def json(self):
        return self._payload


class FakeClient:
    """Stands in for httpx.AsyncClient; scripted responses, recorded calls."""

    def __init__(self, post_responses=None, get_responses=None):
        self.post_responses = list(post_responses or [])
        self.get_responses = dict(get_responses or {})
        self.posts = []
        self.gets = []

    async def post(self, url, json=None, headers=None):
        self.posts.append({"url": url, "json": json, "headers": headers or {}})
        if not self.post_responses:
            return FakeResponse(202, {"dm_id": f"dm_{len(self.posts)}", "status": "queued"})
        nxt = self.post_responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    async def get(self, url):
        self.gets.append(url)
        dm_id = url.rsplit("/", 1)[-1]
        return self.get_responses.get(dm_id, FakeResponse(200, {"status": "queued"}))


@pytest.fixture
def fake_client(monkeypatch):
    """Install a FakeClient in both sender and reconciler."""
    from app import reconciler, sender

    holder = {}

    def install(client):
        holder["client"] = client
        monkeypatch.setattr(sender, "client", lambda: client)
        monkeypatch.setattr(reconciler, "client", lambda: client)
        return client

    return install
