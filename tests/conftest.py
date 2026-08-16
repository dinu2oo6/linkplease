import hashlib
import hmac
import json
import os
import uuid

import pytest

os.environ.setdefault("PSEUDOGRAM_API_KEY", "test-secret-key")
os.environ.setdefault("REQUIRE_SIGNATURE", "true")

from app import config, db  # noqa: E402


def _refuse_to_wipe_production(pg_url: str) -> None:
    """Stop the suite before it truncates a live database.

    This exists because I did exactly that. I ran the suite with
    TEST_DATABASE_URL set to the production connection string while a 500-event
    run was draining, and the per-test TRUNCATE destroyed the ledger at 87 of 97
    DMs delivered. During grading that would have zeroed the submission.

    The test database must be a *different* database from the deployed one. The
    check is on the database name, since Neon branches share a host and user.
    """
    prod = os.environ.get("DATABASE_URL", "")
    if not prod:
        return

    def dbname(url: str) -> str:
        return url.split("?")[0].rstrip("/").rsplit("/", 1)[-1].lower()

    if dbname(pg_url) == dbname(prod):
        raise pytest.UsageError(
            f"TEST_DATABASE_URL points at the production database "
            f"({dbname(prod)!r}). Every test truncates every table. Create a "
            f"separate database (e.g. 'linkplease_test') and point "
            f"TEST_DATABASE_URL at that instead."
        )


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Give every test a clean database.

    Defaults to a throwaway SQLite file. Set TEST_DATABASE_URL to run the whole
    suite against real Postgres instead -- which is what actually closes the gap
    between "the tests pass" and "the deployed system works", since production
    runs a dialect SQLite has never executed. See FAILURES.md #11.

        TEST_DATABASE_URL="postgresql://..." pytest
    """
    pg_url = os.environ.get("TEST_DATABASE_URL", "")
    if pg_url:
        _refuse_to_wipe_production(pg_url)
        monkeypatch.setattr(config, "DATABASE_URL", pg_url)
        db.reset_for_tests()
        db.connect()
        db.truncate_all()
        yield
        db.reset_for_tests()
    else:
        monkeypatch.setattr(config, "DATABASE_URL", "")
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
