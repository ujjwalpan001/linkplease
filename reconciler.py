"""
Delivery reconciler.

The /v1/dm/send endpoint returns 202 (accepted), not delivered.
~15% of accepted DMs silently fail. This thread polls GET /v1/dm/{dm_id}
every RECONCILER_INTERVAL seconds for all DMs in 'accepted' status and:
  - delivered → mark 'sent'
  - failed    → re-queue (up to MAX_ATTEMPTS), then 'failed'
  - queued    → leave alone (not yet terminal)

GET /v1/dm/{dm_id} does NOT count against the rate limit per the spec.
"""

import logging
import time
import threading
from datetime import datetime, timezone

import requests

import config
import db

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _future_iso(seconds: float) -> str:
    return datetime.fromtimestamp(time.time() + seconds, tz=timezone.utc).isoformat()


class Reconciler:
    def __init__(self, interval: float = config.RECONCILER_INTERVAL):
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="reconciler"
        )

    def start(self) -> None:
        self._thread.start()
        logger.info("Reconciler started")

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._reconcile()
            except Exception:
                logger.exception("Reconciler unhandled error")
            self._stop.wait(self.interval)

    def _reconcile(self) -> None:
        conn = db.get_conn()
        rows = conn.execute(
            "SELECT id, dm_id, attempts FROM dm_queue WHERE status='accepted' AND dm_id IS NOT NULL"
        ).fetchall()

        if not rows:
            return

        logger.debug(f"Reconciling {len(rows)} accepted DMs")

        for row in rows:
            try:
                resp = requests.get(
                    f"{config.BASE_URL}/v1/dm/{row['dm_id']}",
                    headers={"X-API-Key": config.API_KEY},
                    timeout=10,
                )
            except requests.RequestException as exc:
                logger.warning(f"Reconciler network error for {row['dm_id']}: {exc}")
                continue

            if resp.status_code != 200:
                continue

            status = resp.json().get("status")
            now = _now_iso()

            if status == "delivered":
                conn.execute(
                    "UPDATE dm_queue SET status='sent', updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
                conn.commit()
                logger.info(f"DM {row['dm_id']} confirmed delivered")

            elif status == "failed":
                attempts = row["attempts"]
                if attempts < config.MAX_ATTEMPTS:
                    backoff = min(config.BACKOFF_BASE ** attempts, 300)
                    conn.execute(
                        """UPDATE dm_queue
                           SET status='pending', dm_id=NULL,
                               next_attempt_at=?, updated_at=?
                           WHERE id=?""",
                        (_future_iso(backoff), now, row["id"]),
                    )
                    logger.warning(
                        f"DM {row['dm_id']} failed delivery — re-queued "
                        f"(attempt {attempts}, backoff {backoff:.0f}s)"
                    )
                else:
                    conn.execute(
                        "UPDATE dm_queue SET status='failed', updated_at=? WHERE id=?",
                        (now, row["id"]),
                    )
                    logger.error(
                        f"DM {row['dm_id']} permanently failed after reconciliation"
                    )
                conn.commit()
            # status == 'queued' → still in-flight, leave alone
