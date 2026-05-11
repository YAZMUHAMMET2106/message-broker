"""
Database layer for message persistence.
Uses SQLite for storing messages and delivery guarantees.
"""

import sqlite3
import threading
import time
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class MessageDatabase:
    """Thread-safe SQLite database for message persistence."""

    def __init__(self, db_path: str = "broker.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_db(self):
        """Initialize database schema."""
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                topic       TEXT NOT NULL,
                payload     TEXT NOT NULL,
                producer_id TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                delivery_mode TEXT NOT NULL DEFAULT 'at_least_once',
                created_at  REAL NOT NULL,
                delivered_at REAL,
                ack_at      REAL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                next_retry_at REAL
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                topic       TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                created_at  REAL NOT NULL,
                UNIQUE(topic, consumer_id)
            );

            CREATE TABLE IF NOT EXISTS delivery_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id  INTEGER NOT NULL,
                consumer_id TEXT NOT NULL,
                status      TEXT NOT NULL,
                attempt     INTEGER NOT NULL DEFAULT 1,
                timestamp   REAL NOT NULL,
                error       TEXT,
                FOREIGN KEY(message_id) REFERENCES messages(id)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
            CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
            CREATE INDEX IF NOT EXISTS idx_delivery_log_msg ON delivery_log(message_id);
        """)
        conn.commit()
        logger.info("Database initialized: %s", self.db_path)

    # ──────────────────────── Messages ────────────────────────

    def save_message(self, topic: str, payload: str, producer_id: str,
                     delivery_mode: str = "at_least_once",
                     max_retries: int = 3) -> int:
        """Persist a new message and return its ID."""
        conn = self._get_conn()
        now = time.time()
        cur = conn.execute(
            """INSERT INTO messages
               (topic, payload, producer_id, status, delivery_mode,
                created_at, max_retries, next_retry_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (topic, payload, producer_id, "pending", delivery_mode,
             now, max_retries, now)
        )
        conn.commit()
        msg_id = cur.lastrowid
        logger.debug("Saved message id=%d topic=%s", msg_id, topic)
        return msg_id

    def get_pending_messages(self, topic: str) -> List[Dict[str, Any]]:
        """Fetch messages ready to be delivered."""
        conn = self._get_conn()
        now = time.time()
        rows = conn.execute(
            """SELECT * FROM messages
               WHERE topic=? AND status='pending' AND next_retry_at<=?
               ORDER BY created_at ASC""",
            (topic, now)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_delivered(self, message_id: int):
        """Mark message as delivered (waiting for ACK)."""
        conn = self._get_conn()
        conn.execute(
            "UPDATE messages SET status='delivered', delivered_at=? WHERE id=?",
            (time.time(), message_id)
        )
        conn.commit()

    def mark_acknowledged(self, message_id: int):
        """Mark message as acknowledged by consumer."""
        conn = self._get_conn()
        conn.execute(
            "UPDATE messages SET status='acknowledged', ack_at=? WHERE id=?",
            (time.time(), message_id)
        )
        conn.commit()
        logger.info("Message id=%d acknowledged", message_id)

    def mark_failed(self, message_id: int, error: str = ""):
        """Mark message as permanently failed."""
        conn = self._get_conn()
        conn.execute(
            "UPDATE messages SET status='failed' WHERE id=?",
            (message_id,)
        )
        conn.commit()
        logger.warning("Message id=%d failed: %s", message_id, error)

    def increment_retry(self, message_id: int, delay: float = 5.0) -> bool:
        """
        Increment retry count. Returns True if more retries are allowed,
        False if max retries exceeded.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT retry_count, max_retries FROM messages WHERE id=?",
            (message_id,)
        ).fetchone()
        if not row:
            return False

        new_count = row["retry_count"] + 1
        if new_count >= row["max_retries"]:
            self.mark_failed(message_id, "max retries exceeded")
            return False

        next_retry = time.time() + delay
        conn.execute(
            """UPDATE messages
               SET retry_count=?, status='pending', next_retry_at=?
               WHERE id=?""",
            (new_count, next_retry, message_id)
        )
        conn.commit()
        logger.info("Message id=%d retry %d/%d scheduled in %.1fs",
                    message_id, new_count, row["max_retries"], delay)
        return True

    # ──────────────────────── Subscriptions ───────────────────

    def add_subscription(self, topic: str, consumer_id: str):
        conn = self._get_conn()
        conn.execute(
            """INSERT OR IGNORE INTO subscriptions (topic, consumer_id, created_at)
               VALUES (?,?,?)""",
            (topic, consumer_id, time.time())
        )
        conn.commit()

    def remove_subscription(self, topic: str, consumer_id: str):
        conn = self._get_conn()
        conn.execute(
            "DELETE FROM subscriptions WHERE topic=? AND consumer_id=?",
            (topic, consumer_id)
        )
        conn.commit()

    def get_subscribers(self, topic: str) -> List[str]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT consumer_id FROM subscriptions WHERE topic=?", (topic,)
        ).fetchall()
        return [r["consumer_id"] for r in rows]

    # ──────────────────────── Delivery log ────────────────────

    def log_delivery(self, message_id: int, consumer_id: str,
                     status: str, attempt: int = 1, error: str = ""):
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO delivery_log
               (message_id, consumer_id, status, attempt, timestamp, error)
               VALUES (?,?,?,?,?,?)""",
            (message_id, consumer_id, status, attempt, time.time(), error)
        )
        conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        conn = self._get_conn()
        stats = {}
        for status in ("pending", "delivered", "acknowledged", "failed"):
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM messages WHERE status=?", (status,)
            ).fetchone()
            stats[status] = row["cnt"]
        row = conn.execute("SELECT COUNT(*) as cnt FROM subscriptions").fetchone()
        stats["subscriptions"] = row["cnt"]
        return stats
