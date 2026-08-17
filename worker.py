"""
Background DM sender worker.

Runs as a single daemon thread so rate-limit accounting is trivially correct:
one thread = one rate-limit context, no need for a shared semaphore.

Rate limiter: true sliding window (not a simple token bucket).
  - Tracks timestamps of the last N calls in a deque
  - Before each call, evicts timestamps older than 60s
  - If len(deque) >= 10, sleeps until the oldest timestamp falls out
  - This prevents the double-burst problem at refill boundaries
  - On API 429: additionally sleeps Retry-After seconds
"""

import collections
import logging
import sqlite3
import time
import threading
from datetime import datetime, timezone

import requests

import config
import db

logger = logging.getLogger(__name__)


class SlidingWindowRateLimiter:
    """
    True sliding-window rate limiter.

    Tracks call timestamps in a deque. Before each call:
      1. Evict timestamps older than WINDOW seconds.
      2. If count >= LIMIT, sleep until the oldest timestamp ages out.
      3. Record the new call timestamp.

    This guarantees at most LIMIT calls in any rolling WINDOW-second period,
    unlike a token bucket which can allow 2×LIMIT calls at a refill boundary.
    """

    def __init__(self, limit: int, window: float):
        self.limit = limit
        self.window = window
        self._calls: collections.deque[float] = collections.deque()
        self._lock = threading.Lock()

    def wait_and_consume(self) -> None:
        """Block until a call is allowed within the sliding window, then record it."""
        while True:
            with self._lock:
                now = time.monotonic()
                # Evict timestamps outside the rolling window
                while self._calls and now - self._calls[0] >= self.window:
                    self._calls.popleft()

                if len(self._calls) < self.limit:
                    self._calls.append(now)
                    return

                # Sleep until the oldest call falls out of the window
                sleep_for = self.window - (now - self._calls[0]) + 0.01
            time.sleep(max(sleep_for, 0.1))


def _future_iso(seconds: float) -> str:
    return datetime.fromtimestamp(time.time() + seconds, tz=timezone.utc).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DMWorker:
    def __init__(self):
        self._bucket = SlidingWindowRateLimiter(config.RATE_LIMIT_CALLS, config.RATE_LIMIT_WINDOW)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="dm-worker")

    def start(self) -> None:
        self._thread.start()
        logger.info("DM worker started")

    def stop(self) -> None:
        self._stop.set()

    # ── main loop ──────────────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                found = self._process_next()
            except Exception:
                logger.exception("Worker unhandled error")
                found = False
            if not found:
                # Nothing ready — short sleep before polling again
                self._stop.wait(0.5)

    def _process_next(self) -> bool:
        """Pick one pending item and attempt to send. Returns True if one was found."""
        conn = db.get_conn()
        now = _now_iso()

        row = conn.execute(
            """
            SELECT id, rule_id, user_id, comment_id, message,
                   idempotency_key, attempts
            FROM   dm_queue
            WHERE  status = 'pending'
              AND  next_attempt_at <= ?
            ORDER  BY next_attempt_at ASC
            LIMIT  1
            """,
            (now,),
        ).fetchone()

        if row is None:
            return False

        # Cancel if the comment was deleted before we sent
        deleted = conn.execute(
            "SELECT 1 FROM deleted_comments WHERE comment_id = ?",
            (row["comment_id"],),
        ).fetchone()

        if deleted:
            conn.execute(
                "UPDATE dm_queue SET status='cancelled', updated_at=? WHERE id=?",
                (_now_iso(), row["id"]),
            )
            conn.commit()
            logger.info(f"DM {row['id']} cancelled (comment deleted)")
            return True

        # Consume a rate-limit token (blocks if needed)
        self._bucket.wait_and_consume()
        self._send(row)
        return True

    # ── send logic ─────────────────────────────────────────────────────────────

    def _send(self, row: sqlite3.Row) -> None:
        conn = db.get_conn()
        attempts = row["attempts"] + 1
        now = _now_iso()

        try:
            resp = requests.post(
                f"{config.BASE_URL}/v1/dm/send",
                json={
                    "recipient_user_id": row["user_id"],
                    "message": row["message"],
                    "comment_id": row["comment_id"],
                },
                headers={
                    "X-API-Key": config.API_KEY,
                    "Idempotency-Key": row["idempotency_key"],
                },
                timeout=15,
            )
        except requests.RequestException as exc:
            logger.warning(f"Network error on DM {row['id']}: {exc}")
            self._reschedule(conn, row["id"], attempts, now)
            return

        if resp.status_code in (200, 202):
            dm_id = resp.json().get("dm_id")
            conn.execute(
                "UPDATE dm_queue SET status='accepted', dm_id=?, attempts=?, updated_at=? WHERE id=?",
                (dm_id, attempts, now, row["id"]),
            )
            conn.commit()
            logger.info(f"DM accepted: dm_id={dm_id} user={row['user_id']}")

        elif resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 60))
            conn.execute(
                "UPDATE dm_queue SET attempts=?, next_attempt_at=?, updated_at=? WHERE id=?",
                (attempts, _future_iso(retry_after), now, row["id"]),
            )
            conn.commit()
            logger.warning(f"Rate limited by API — sleeping {retry_after}s")
            time.sleep(retry_after)

        elif resp.status_code == 500:
            # Transient; exponential backoff
            self._reschedule(conn, row["id"], attempts, now)

        elif resp.status_code in (400, 401):
            # Non-retriable: malformed payload or bad API key
            conn.execute(
                "UPDATE dm_queue SET status='failed', attempts=?, updated_at=? WHERE id=?",
                (attempts, now, row["id"]),
            )
            conn.commit()
            logger.error(f"DM {row['id']} failed permanently ({resp.status_code}): {resp.text}")

        else:
            logger.warning(f"Unexpected status {resp.status_code} for DM {row['id']}")
            self._reschedule(conn, row["id"], attempts, now)

    def _reschedule(
        self, conn: sqlite3.Connection, dm_id: int, attempts: int, now: str
    ) -> None:
        if attempts >= config.MAX_ATTEMPTS:
            conn.execute(
                "UPDATE dm_queue SET status='failed', attempts=?, updated_at=? WHERE id=?",
                (attempts, now, dm_id),
            )
            logger.error(f"DM {dm_id} permanently failed after {attempts} attempts")
        else:
            backoff = min(config.BACKOFF_BASE ** attempts, 300)
            conn.execute(
                "UPDATE dm_queue SET attempts=?, next_attempt_at=?, updated_at=? WHERE id=?",
                (attempts, _future_iso(backoff), now, dm_id),
            )
            logger.warning(f"DM {dm_id} rescheduled in {backoff:.0f}s (attempt {attempts})")
        conn.commit()
