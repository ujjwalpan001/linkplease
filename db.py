"""
SQLite database layer.

Connection strategy: one connection per thread (thread-local), opened lazily.
WAL mode allows concurrent reads alongside a single writer, which is all we need.
busy_timeout prevents "database is locked" errors under write contention.
"""
import sqlite3
import threading
from contextlib import contextmanager
from typing import Generator

import config

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Return the thread-local SQLite connection, creating it if needed."""
    if not hasattr(_local, "conn"):
        conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")  # 5 s wait on contention
        _local.conn = conn
    return _local.conn


@contextmanager
def transaction() -> Generator[sqlite3.Connection, None, None]:
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db() -> None:
    """Create all tables on first run. Safe to call multiple times."""
    with transaction() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS rules (
                rule_id    TEXT PRIMARY KEY,
                keyword    TEXT NOT NULL,       -- stored lowercase
                dm_message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            -- Prevents re-processing re-delivered webhook events.
            CREATE TABLE IF NOT EXISTS seen_events (
                event_id    TEXT PRIMARY KEY,
                received_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dm_queue (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id          TEXT NOT NULL,
                user_id          TEXT NOT NULL,
                comment_id       TEXT NOT NULL,
                message          TEXT NOT NULL,
                -- UNIQUE constraint is our concurrency-safe dedup gate.
                -- Two threads racing on the same (rule, user) pair will both
                -- attempt INSERT; the second one gets IntegrityError and is
                -- counted as a duplicate instead of sending twice.
                idempotency_key  TEXT UNIQUE NOT NULL,  -- "{rule_id}:{user_id}"
                status           TEXT NOT NULL DEFAULT 'pending',
                -- status values: pending | accepted | sent | failed | cancelled
                dm_id            TEXT,           -- returned by /v1/dm/send 202
                attempts         INTEGER NOT NULL DEFAULT 0,
                next_attempt_at  TEXT NOT NULL,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL,
                FOREIGN KEY (rule_id) REFERENCES rules(rule_id)
            );

            CREATE INDEX IF NOT EXISTS idx_dmq_status_next
                ON dm_queue (status, next_attempt_at);

            -- comment.deleted events register here so the worker can
            -- cancel pending DMs before they go out.
            CREATE TABLE IF NOT EXISTS deleted_comments (
                comment_id TEXT PRIMARY KEY,
                deleted_at TEXT NOT NULL
            );

            -- Persistent counter that survives process restarts.
            CREATE TABLE IF NOT EXISTS counters (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO counters (key, value)
                VALUES ('duplicates_blocked', 0);
        """)
