"""Runtime configuration, read once from the environment at import time."""
import os


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


# --- PseudoGram credentials -------------------------------------------------
# The API key doubles as the HMAC secret for inbound webhook signatures.
API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")
BASE_URL = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com").rstrip("/")
ACCOUNT_EMAIL = os.getenv("PSEUDOGRAM_EMAIL", "")
# Our own public /webhook URL, used when kicking off a simulation from /admin.
PUBLIC_WEBHOOK_URL = os.getenv("PUBLIC_WEBHOOK_URL", "")

# --- Storage ----------------------------------------------------------------
# Two backends, one set of SQL. Postgres when DATABASE_URL is set (production,
# because no free host offers a persistent disk any more), SQLite otherwise
# (local dev and the test suite, because it needs no server).
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "./data/linkplease.db")


def use_postgres() -> bool:
    return bool(DATABASE_URL)

# --- Webhook security (Part B) ---------------------------------------------
# Off only for local development against the chaos server, which signs nothing.
REQUIRE_SIGNATURE = _bool("REQUIRE_SIGNATURE", True)
SIGNATURE_HEADER = "x-pseudogram-signature"

# --- Rate governor ----------------------------------------------------------
# PseudoGram allows 10 sends per rolling 60s. We pace evenly instead of bursting:
# one send every SEND_INTERVAL_SECONDS is ~9.8/min, which keeps us under the
# limit even if our clock disagrees with theirs by a couple of seconds.
SEND_INTERVAL_SECONDS = _float("SEND_INTERVAL_SECONDS", 6.1)
RATE_LIMIT_MAX = _int("RATE_LIMIT_MAX", 10)
RATE_LIMIT_WINDOW = _float("RATE_LIMIT_WINDOW", 60.0)
# Hard backstop: never let the rolling window reach the real ceiling.
RATE_LIMIT_HEADROOM = _int("RATE_LIMIT_HEADROOM", 1)

# --- Retry policy -----------------------------------------------------------
MAX_SEND_ATTEMPTS = _int("MAX_SEND_ATTEMPTS", 6)
BACKOFF_BASE_SECONDS = _float("BACKOFF_BASE_SECONDS", 1.0)
BACKOFF_CAP_SECONDS = _float("BACKOFF_CAP_SECONDS", 300.0)
HTTP_TIMEOUT_SECONDS = _float("HTTP_TIMEOUT_SECONDS", 15.0)

# --- Reconciliation (Part C) ------------------------------------------------
RECONCILE_INTERVAL_SECONDS = _float("RECONCILE_INTERVAL_SECONDS", 5.0)
# A DM the API accepted but never resolves is treated as stuck and resent.
RECONCILE_STUCK_SECONDS = _float("RECONCILE_STUCK_SECONDS", 180.0)
MAX_RESENDS = _int("MAX_RESENDS", 3)

# --- Keep-alive -------------------------------------------------------------
# Free hosts suspend an idle service. 10 minutes is comfortably inside Render's
# ~15 minute window. Set KEEPALIVE_URL to disable derivation, or "off".
KEEPALIVE_URL = os.getenv("KEEPALIVE_URL", "")
KEEPALIVE_INTERVAL_SECONDS = _float("KEEPALIVE_INTERVAL_SECONDS", 600.0)


def keepalive_url() -> str:
    """Our own /health URL, derived from the public webhook URL if not set."""
    if KEEPALIVE_URL:
        return "" if KEEPALIVE_URL.lower() == "off" else KEEPALIVE_URL
    if PUBLIC_WEBHOOK_URL.endswith("/webhook"):
        return PUBLIC_WEBHOOK_URL[: -len("/webhook")] + "/health"
    return ""


# How far back "is this happening now?" looks, for the invariants panel.
INVARIANT_RECENT_WINDOW = _float("INVARIANT_RECENT_WINDOW", 900.0)

# --- Ingest batching --------------------------------------------------------
# Requests arriving within this window share one INSERT. Costs an idle event
# ~15ms; saves a 500-event burst from serialising 500 network round-trips.
INGEST_BATCH_WINDOW = _float("INGEST_BATCH_WINDOW", 0.015)
INGEST_BATCH_MAX = _int("INGEST_BATCH_MAX", 100)

# --- Matcher ----------------------------------------------------------------
MATCH_INTERVAL_SECONDS = _float("MATCH_INTERVAL_SECONDS", 0.25)
MATCH_BATCH_SIZE = _int("MATCH_BATCH_SIZE", 200)
