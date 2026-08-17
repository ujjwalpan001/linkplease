"""
FastAPI application — LinkPlease webhook automation service.

Routes
------
POST /webhook  — receives comment events; returns 200 in <5 ms, processes async
POST /rules    — create a keyword → DM rule
GET  /stats    — live delivery numbers
"""

import collections
import hashlib
import hmac
import logging
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

import config
import db
from reconciler import Reconciler
from worker import DMWorker

# In-memory logging buffer to debug deployed app
class MemoryHandler(logging.Handler):
    def __init__(self, capacity=200):
        super().__init__()
        self.buffer = collections.deque(maxlen=capacity)
    def emit(self, record):
        self.buffer.append(self.format(record))

mem_handler = MemoryHandler()
mem_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s"))
logging.getLogger().addHandler(mem_handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Startup / shutdown ────────────────────────────────────────────────────────

worker = DMWorker()
reconciler = Reconciler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    worker.start()
    reconciler.start()
    logger.info("LinkPlease started")
    yield
    worker.stop()
    reconciler.stop()
    logger.info("LinkPlease stopped")


app = FastAPI(title="LinkPlease", version="1.0.0", lifespan=lifespan)


# ── Pydantic models ───────────────────────────────────────────────────────────

class WebhookData(BaseModel):
    comment_id: str = ""
    text: str = ""
    from_user: dict = {}


class WebhookPayload(BaseModel):
    event_id: str
    event_type: str  # "comment.created" or "comment.deleted"
    data: dict = {}


class RuleIn(BaseModel):
    keyword: str
    dm_message: str


class RuleOut(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _verify_signature(raw_body: bytes, header: str) -> bool:
    """Return True if the HMAC-SHA256 signature matches."""
    key = config.API_KEY
    if not key:
        logger.info("Signature verification skipped (no API key configured)")
        return True  # skip verification when no key is configured (local dev)
    expected = "sha256=" + hmac.new(
        key.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    
    masked_key = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "too_short"
    logger.info(
        f"Signature check: Key len={len(key)} ({masked_key}), "
        f"body len={len(raw_body)}, "
        f"header={header}, "
        f"expected={expected}"
    )
    return hmac.compare_digest(expected, header)


# ── Background task (runs after 200 is returned) ──────────────────────────────

def _process_webhook(payload: dict) -> None:
    """
    Match a comment event against rules and enqueue DMs.
    Runs in FastAPI's threadpool (sync background task).
    """
    event_id = payload.get("event_id", "")
    event_type = payload.get("event_type", "")

    if not event_id:
        logger.warning("Webhook missing event_id")
        return

    conn = db.get_conn()
    now = _now_iso()

    # ── Idempotency: deduplicate re-delivered events ──────────────────────────
    try:
        conn.execute(
            "INSERT INTO seen_events (event_id, received_at) VALUES (?, ?)",
            (event_id, now),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        logger.debug(f"Duplicate event {event_id!r} — skipped")
        return

    data = payload.get("data", {})

    # ── comment.deleted ───────────────────────────────────────────────────────
    if event_type == "comment.deleted":
        comment_id = data.get("comment_id")
        if comment_id:
            conn.execute(
                "INSERT OR IGNORE INTO deleted_comments (comment_id, deleted_at) VALUES (?, ?)",
                (comment_id, now),
            )
            # Cancel pending DMs for this comment before they go out
            conn.execute(
                "UPDATE dm_queue SET status='cancelled', updated_at=? "
                "WHERE comment_id=? AND status='pending'",
                (now, comment_id),
            )
            conn.commit()
            logger.info(f"comment.deleted: {comment_id}")
        return

    if event_type != "comment.created":
        return

    # ── Extract fields ────────────────────────────────────────────────────────
    comment_id = data.get("comment_id", "")
    comment_text = data.get("text", "")
    user_id = data.get("from", {}).get("user_id", "")

    if not (comment_id and comment_text and user_id):
        logger.warning(f"Incomplete comment.created payload: {data}")
        return

    text_lower = comment_text.lower()
    rules = conn.execute(
        "SELECT rule_id, keyword, dm_message FROM rules"
    ).fetchall()

    for rule in rules:
        if rule["keyword"] not in text_lower:
            continue

        # idempotency_key is UNIQUE in dm_queue — the DB enforces exactly-once
        # semantics even if two threads race on the same (rule, user) pair.
        idem_key = f"{rule['rule_id']}:{user_id}"

        try:
            conn.execute(
                """
                INSERT INTO dm_queue
                    (rule_id, user_id, comment_id, message, idempotency_key,
                     status, attempts, next_attempt_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    rule["rule_id"], user_id, comment_id, rule["dm_message"],
                    idem_key, now, now, now,
                ),
            )
            conn.commit()
            logger.info(f"DM queued: rule={rule['rule_id']} user={user_id}")

        except sqlite3.IntegrityError:
            # This (rule, user) was already sent/queued — count it and move on
            conn.execute(
                "UPDATE counters SET value = value + 1 WHERE key = 'duplicates_blocked'"
            )
            conn.commit()
            logger.debug(f"Duplicate DM blocked: rule={rule['rule_id']} user={user_id}")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/webhook", status_code=200)
async def webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_pseudogram_signature: str = Header(default="", description="HMAC-SHA256 signature. Format: sha256=<hex>. Leave blank if API key is not configured."),
    body: WebhookPayload = None,
):
    """
    Receive a comment event from PseudoGram.
    Returns 200 immediately; all processing happens in a background task
    so we never block longer than the signature check (~1 ms).

    **Signature:** Production requests include `X-PseudoGram-Signature: sha256=<hex>` header.
    Leave blank for local testing (signature check is skipped when API key is not configured).
    """
    # Starlette caches the body after first read, so this works correctly
    # alongside the Pydantic body parameter above.
    raw_body = await request.body()

    # Part B — reject forged requests
    sig = request.headers.get("X-PseudoGram-Signature", "")
    if not _verify_signature(raw_body, sig):
        logger.warning("Rejected request: invalid signature")
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    background_tasks.add_task(_process_webhook, payload)
    return {"ok": True}


@app.post("/rules", status_code=201, response_model=RuleOut)
def create_rule(body: RuleIn) -> RuleOut:
    """Create a keyword → DM rule. Keyword matching is case-insensitive."""
    rule_id = str(uuid.uuid4())
    keyword = body.keyword.lower().strip()
    now = _now_iso()

    conn = db.get_conn()
    conn.execute(
        "INSERT INTO rules (rule_id, keyword, dm_message, created_at) VALUES (?, ?, ?, ?)",
        (rule_id, keyword, body.dm_message, now),
    )
    conn.commit()

    logger.info(f"Rule created: {rule_id!r} keyword={keyword!r}")
    return RuleOut(rule_id=rule_id, keyword=keyword, dm_message=body.dm_message)


@app.get("/stats")
def get_stats() -> dict:
    """
    Live delivery numbers.
    sent              — DMs confirmed delivered by the mock API
    failed            — gave up after MAX_ATTEMPTS retries
    queued            — pending or accepted (in-flight)
    duplicates_blocked — same (user, rule) pair seen more than once
    """
    conn = db.get_conn()

    sent = conn.execute(
        "SELECT COUNT(*) FROM dm_queue WHERE status='sent'"
    ).fetchone()[0]

    failed = conn.execute(
        "SELECT COUNT(*) FROM dm_queue WHERE status='failed'"
    ).fetchone()[0]

    queued = conn.execute(
        "SELECT COUNT(*) FROM dm_queue WHERE status IN ('pending','accepted')"
    ).fetchone()[0]

    duplicates_blocked = conn.execute(
        "SELECT value FROM counters WHERE key='duplicates_blocked'"
    ).fetchone()[0]

    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }


@app.get("/")
def health_check():
    """Render health check endpoint."""
    return {"status": "ok"}


@app.get("/logs")
def get_logs():
    """Expose recent application logs."""
    return list(mem_handler.buffer)
