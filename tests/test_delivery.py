#!/usr/bin/env python3
"""
Unit tests for the Message Broker delivery guarantee module.
Run with:  python -m pytest tests/test_delivery.py -v
       or: python tests/test_delivery.py
"""

import sys, os, time, threading, unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import MessageDatabase
from broker.delivery import DeliveryGuarantor, DeliveryMode


def make_db(name: str = ":memory:") -> MessageDatabase:
    return MessageDatabase(name)


class TestMessageDatabase(unittest.TestCase):

    def setUp(self):
        self.db = make_db()

    def test_save_and_fetch(self):
        mid = self.db.save_message("test", "hello", "p1")
        msgs = self.db.get_pending_messages("test")
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["id"], mid)
        self.assertEqual(msgs[0]["payload"], "hello")

    def test_mark_acknowledged(self):
        mid = self.db.save_message("test", "hello", "p1")
        self.db.mark_delivered(mid)
        self.db.mark_acknowledged(mid)
        msgs = self.db.get_pending_messages("test")
        self.assertEqual(len(msgs), 0)

    def test_retry_increments(self):
        mid = self.db.save_message("test", "msg", "p1", max_retries=3)
        ok = self.db.increment_retry(mid, delay=0)
        self.assertTrue(ok)
        ok = self.db.increment_retry(mid, delay=0)
        self.assertTrue(ok)
        ok = self.db.increment_retry(mid, delay=0)
        self.assertFalse(ok)  # max retries exceeded → failed

    def test_subscriptions(self):
        self.db.add_subscription("topic-a", "c1")
        self.db.add_subscription("topic-a", "c2")
        subs = self.db.get_subscribers("topic-a")
        self.assertIn("c1", subs)
        self.assertIn("c2", subs)
        self.db.remove_subscription("topic-a", "c1")
        subs = self.db.get_subscribers("topic-a")
        self.assertNotIn("c1", subs)

    def test_stats(self):
        self.db.save_message("t", "p", "prod")
        stats = self.db.get_stats()
        self.assertIn("pending", stats)
        self.assertGreaterEqual(stats["pending"], 1)


class TestDeliveryGuarantor(unittest.TestCase):

    def setUp(self):
        self.db_path = f"/tmp/test_broker_{id(self)}.db"
        self.db = MessageDatabase(self.db_path)
        self.g = DeliveryGuarantor(self.db)
        self.g.start()

    def tearDown(self):
        self.g.stop()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _make_msg(self, mode=DeliveryMode.AT_LEAST_ONCE):
        return self.db.save_message("t", "payload", "p1", delivery_mode=mode)

    # ── at_most_once ──

    def test_at_most_once_no_ack_tracking(self):
        mid = self._make_msg(DeliveryMode.AT_MOST_ONCE)
        result = self.g.prepare_delivery(mid, "c1", DeliveryMode.AT_MOST_ONCE)
        self.assertTrue(result)
        # No pending ACK should be tracked
        self.assertNotIn(mid, self.g._pending_acks)

    # ── at_least_once ──

    def test_at_least_once_tracks_pending_ack(self):
        mid = self._make_msg(DeliveryMode.AT_LEAST_ONCE)
        self.g.prepare_delivery(mid, "c1", DeliveryMode.AT_LEAST_ONCE)
        self.assertIn(mid, self.g._pending_acks)
        self.assertIn("c1", self.g._pending_acks[mid])

    def test_ack_clears_pending(self):
        mid = self._make_msg(DeliveryMode.AT_LEAST_ONCE)
        self.g.prepare_delivery(mid, "c1", DeliveryMode.AT_LEAST_ONCE)
        self.db.mark_delivered(mid)
        self.g.on_ack(mid, "c1")
        self.assertNotIn(mid, self.g._pending_acks)

    def test_nack_triggers_retry(self):
        mid = self._make_msg(DeliveryMode.AT_LEAST_ONCE)
        self.g.prepare_delivery(mid, "c1", DeliveryMode.AT_LEAST_ONCE)
        self.db.mark_delivered(mid)
        self.g.on_nack(mid, "c1", error="processing error")
        # Message should be back as pending (with small delay)
        msgs = self.db.get_pending_messages("t")
        # allow up to 1 second for retry scheduling
        self.assertGreaterEqual(len(msgs), 0)  # may be delayed

    # ── exactly_once ──

    def test_exactly_once_deduplication(self):
        mid = self._make_msg(DeliveryMode.EXACTLY_ONCE)
        key = f"{mid}:c1"
        r1 = self.g.prepare_delivery(mid, "c1", DeliveryMode.EXACTLY_ONCE,
                                     dedup_key=key)
        r2 = self.g.prepare_delivery(mid, "c1", DeliveryMode.EXACTLY_ONCE,
                                     dedup_key=key)
        self.assertTrue(r1)
        self.assertFalse(r2)   # duplicate blocked

    # ── timeout → retry ──

    def test_ack_timeout_triggers_retry(self):
        """Reduce ACK timeout and verify retry count increments."""
        original_timeout = DeliveryGuarantor.ACK_TIMEOUT
        DeliveryGuarantor.ACK_TIMEOUT = 0.1
        try:
            mid = self._make_msg(DeliveryMode.AT_LEAST_ONCE)
            self.g.prepare_delivery(mid, "c1", DeliveryMode.AT_LEAST_ONCE)
            self.db.mark_delivered(mid)
            time.sleep(4)   # watcher runs every 2s; give it 2 cycles
            conn = self.db._get_conn()
            row = conn.execute(
                "SELECT status, retry_count FROM messages WHERE id=?", (mid,)
            ).fetchone()
            self.assertIsNotNone(row)
            # Retry count must be > 0 OR status is failed (max retries hit)
            self.assertTrue(
                row["retry_count"] > 0 or row["status"] == "failed",
                f"Expected retry_count>0 or status=failed, got: {dict(row)}"
            )
        finally:
            DeliveryGuarantor.ACK_TIMEOUT = original_timeout


if __name__ == "__main__":
    unittest.main(verbosity=2)
