#!/usr/bin/env python3
"""
Demo: Message Broker — Delivery Guarantees
==========================================
Demonstrates all three delivery modes:
  1. at_most_once  — fire and forget
  2. at_least_once — retry until ACK
  3. exactly_once  — no duplicates
"""

import sys
import os
import time
import threading
import logging

sys.path.insert(0, os.path.dirname(__file__))

from broker.server import BrokerServer
from client.client import Producer, Consumer

logging.basicConfig(
    level=logging.WARNING,   # keep demo output clean
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ────────────────────────── helpers ──────────────────────────

def start_broker():
    srv = BrokerServer(host="127.0.0.1", port=9998, db_path="demo.db")
    t = threading.Thread(target=srv.start, daemon=True, name="broker")
    t.start()
    time.sleep(0.4)          # wait for broker to bind
    return srv


def make_handler(label: str, fail_first: bool = False):
    """Factory for message handler functions."""
    call_count = {"n": 0}

    def handler(msg_id, topic, payload):
        call_count["n"] += 1
        if fail_first and call_count["n"] == 1:
            print(f"  [{label}] msg={msg_id} — simulating failure (NACK)")
            return False      # NACK → broker will retry
        print(f"  [{label}] msg={msg_id} topic={topic} payload={payload!r}")
        return True           # ACK
    return handler


# ────────────────────────── demo sections ────────────────────

def demo_at_most_once(host, port):
    print("\n" + "═"*55)
    print("  MODE: at_most_once  (fire and forget)")
    print("═"*55)

    c = Consumer("consumer-amo", host=host, port=port)
    c.connect()
    c.subscribe("amo-topic", make_handler("AMO"))
    c.listen(blocking=False)

    p = Producer(host=host, port=port)
    p.connect()
    for i in range(3):
        mid = p.publish("amo-topic", f"Hello #{i}", mode="at_most_once",
                        producer_id="prod-amo")
        print(f"  Published msg={mid}")
    p.disconnect()

    time.sleep(2)
    c.stop(); c.disconnect()
    print("  → No ACKs sent (fire and forget)")


def demo_at_least_once(host, port):
    print("\n" + "═"*55)
    print("  MODE: at_least_once  (retry until ACK)")
    print("═"*55)

    # Handler that fails on first call → broker retries
    c = Consumer("consumer-alo", host=host, port=port)
    c.connect()
    c.subscribe("alo-topic", make_handler("ALO", fail_first=True))
    c.listen(blocking=False)

    p = Producer(host=host, port=port)
    p.connect()
    mid = p.publish("alo-topic", "Important message!", mode="at_least_once",
                    producer_id="prod-alo")
    print(f"  Published msg={mid} (consumer will NACK first → expect retry)")
    p.disconnect()

    time.sleep(8)   # wait for retry
    c.stop(); c.disconnect()


def demo_exactly_once(host, port):
    print("\n" + "═"*55)
    print("  MODE: exactly_once  (no duplicates)")
    print("═"*55)

    c = Consumer("consumer-eo", host=host, port=port)
    c.connect()
    c.subscribe("eo-topic", make_handler("EO"))
    c.listen(blocking=False)

    p = Producer(host=host, port=port)
    p.connect()
    mid = p.publish("eo-topic", "Unique order #XYZ", mode="exactly_once",
                    producer_id="prod-eo")
    print(f"  Published msg={mid}")
    p.disconnect()

    time.sleep(3)
    c.stop(); c.disconnect()
    print("  → Dedup key tracked; message delivered exactly once")


def demo_stats(host, port):
    print("\n" + "═"*55)
    print("  BROKER STATS")
    print("═"*55)
    p = Producer(host=host, port=port)
    p.connect()
    stats = p.get_stats()
    p.disconnect()
    for k, v in stats.items():
        print(f"  {k:25s}: {v}")


# ────────────────────────── main ─────────────────────────────

if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 9998

    # Clean up previous demo DB
    if os.path.exists("demo.db"):
        os.remove("demo.db")

    print("\n🚀  Message Broker — Delivery Guarantees Demo")
    srv = start_broker()

    demo_at_most_once(HOST, PORT)
    demo_at_least_once(HOST, PORT)
    demo_exactly_once(HOST, PORT)
    demo_stats(HOST, PORT)

    print("\n✅  Demo finished\n")
