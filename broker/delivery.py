"""
Delivery Guarantee Module
=========================
Implements three delivery semantics:

  • At-most-once  — fire and forget, no retries
  • At-least-once — retry until ACK received (default)
  • Exactly-once  — idempotent delivery via dedup key
"""

import logging
import time
import threading
from enum import Enum
from typing import Callable, Optional, Dict, Set
from database.db import MessageDatabase

logger = logging.getLogger(__name__)


class DeliveryMode(str, Enum):
    AT_MOST_ONCE  = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE  = "exactly_once"


class DeliveryGuarantor:
    """
    Manages delivery guarantees for the broker.

    Responsibilities
    ----------------
    - Track in-flight messages waiting for ACK
    - Retry unacknowledged messages (at_least_once / exactly_once)
    - Deduplicate deliveries for exactly_once semantics
    - Expose ACK / NACK handlers for the broker server
    """

    RETRY_DELAY   = 5.0    # seconds before first retry
    RETRY_BACKOFF = 2.0    # multiply delay on each retry
    ACK_TIMEOUT   = 10.0   # seconds to wait for ACK before retry

    def __init__(self, db: MessageDatabase,
                 send_callback: Optional[Callable] = None):
        self.db = db
        self._send_callback = send_callback          # broker's actual send fn

        # message_id → {consumer_id → deadline}
        self._pending_acks: Dict[int, Dict[str, float]] = {}
        self._lock = threading.Lock()

        # Dedup set for exactly-once: (dedup_key) → message_id
        self._delivered_keys: Dict[str, int] = {}
        self._dedup_keys: Set[str] = set()

        self._running = False
        self._watcher: Optional[threading.Thread] = None

    # ──────────────────────────── Public API ──────────────────────────────

    def start(self):
        """Start the background ACK watcher thread."""
        self._running = True
        self._watcher = threading.Thread(
            target=self._ack_watcher_loop, daemon=True, name="ack-watcher"
        )
        self._watcher.start()
        logger.info("DeliveryGuarantor started")

    def stop(self):
        self._running = False
        if self._watcher:
            self._watcher.join(timeout=3)
        logger.info("DeliveryGuarantor stopped")

    def set_send_callback(self, cb: Callable):
        """Inject the broker send function after construction."""
        self._send_callback = cb

    def prepare_delivery(self, message_id: int, consumer_id: str,
                         mode: DeliveryMode,
                         dedup_key: Optional[str] = None) -> bool:
        """
        Called before the broker sends a message to a consumer.

        Returns
        -------
        True  → proceed with delivery
        False → skip (already delivered for exactly-once)
        """
        if mode == DeliveryMode.EXACTLY_ONCE and dedup_key:
            if dedup_key in self._dedup_keys:
                logger.info("Skipping duplicate: dedup_key=%s", dedup_key)
                return False
            self._dedup_keys.add(dedup_key)

        if mode != DeliveryMode.AT_MOST_ONCE:
            # Track pending ACK
            deadline = time.time() + self.ACK_TIMEOUT
            with self._lock:
                if message_id not in self._pending_acks:
                    self._pending_acks[message_id] = {}
                self._pending_acks[message_id][consumer_id] = deadline

        return True

    def on_ack(self, message_id: int, consumer_id: str):
        """Consumer acknowledged successful delivery."""
        with self._lock:
            if message_id in self._pending_acks:
                self._pending_acks[message_id].pop(consumer_id, None)
                if not self._pending_acks[message_id]:
                    del self._pending_acks[message_id]

        self.db.mark_acknowledged(message_id)
        self.db.log_delivery(message_id, consumer_id, "ack")
        logger.info("ACK  msg=%d consumer=%s", message_id, consumer_id)

    def on_nack(self, message_id: int, consumer_id: str, error: str = ""):
        """Consumer rejected the message — schedule retry."""
        with self._lock:
            if message_id in self._pending_acks:
                self._pending_acks[message_id].pop(consumer_id, None)
                if not self._pending_acks[message_id]:
                    del self._pending_acks[message_id]

        self.db.log_delivery(message_id, consumer_id, "nack", error=error)
        retry_ok = self.db.increment_retry(message_id, delay=self.RETRY_DELAY)
        logger.info("NACK msg=%d consumer=%s retry=%s error=%s",
                    message_id, consumer_id, retry_ok, error)

    # ──────────────────────────── Internal ────────────────────────────────

    def _ack_watcher_loop(self):
        """Periodically check for expired ACK deadlines and trigger retries."""
        logger.info("ACK watcher started")
        while self._running:
            time.sleep(2)
            now = time.time()
            expired = []

            with self._lock:
                for msg_id, consumers in list(self._pending_acks.items()):
                    for consumer_id, deadline in list(consumers.items()):
                        if now > deadline:
                            expired.append((msg_id, consumer_id))

            for msg_id, consumer_id in expired:
                logger.warning("ACK timeout msg=%d consumer=%s → retry",
                               msg_id, consumer_id)
                self.db.log_delivery(msg_id, consumer_id, "timeout")
                retry_ok = self.db.increment_retry(
                    msg_id, delay=self.RETRY_DELAY
                )
                # Remove from in-flight tracking
                with self._lock:
                    if msg_id in self._pending_acks:
                        self._pending_acks[msg_id].pop(consumer_id, None)
                        if not self._pending_acks[msg_id]:
                            del self._pending_acks[msg_id]

                if retry_ok and self._send_callback:
                    # Broker will pick it up on next dispatch cycle
                    pass

    def get_status(self) -> dict:
        with self._lock:
            in_flight = sum(len(v) for v in self._pending_acks.values())
        return {
            "in_flight_acks": in_flight,
            "dedup_keys_tracked": len(self._dedup_keys),
        }
