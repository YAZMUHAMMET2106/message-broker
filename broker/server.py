"""
Message Broker Server
=====================
TCP server implementing the broker protocol.

Protocol (text-based, newline-delimited JSON):
  → {"cmd": "publish",   "topic": "t", "payload": "...", "mode": "at_least_once"}
  → {"cmd": "subscribe", "topic": "t", "consumer_id": "c1"}
  → {"cmd": "ack",       "message_id": 42, "consumer_id": "c1"}
  → {"cmd": "nack",      "message_id": 42, "consumer_id": "c1", "error": "..."}
  → {"cmd": "stats"}
  → {"cmd": "unsubscribe","topic": "t", "consumer_id": "c1"}

  ← {"status": "ok", "message_id": 42}
  ← {"status": "error", "reason": "..."}
  ← {"type": "message", "message_id": 42, "topic": "t", "payload": "..."}
"""

import json
import logging
import socket
import threading
import time
from typing import Dict, Optional

from database.db import MessageDatabase
from broker.delivery import DeliveryGuarantor, DeliveryMode

logger = logging.getLogger(__name__)


class BrokerServer:
    """
    Main broker server.

    Architecture
    ------------
    - One TCP listener thread
    - One thread per connected client
    - One dispatch thread (sends pending messages to subscribers)
    - One ACK watcher thread (inside DeliveryGuarantor)
    """

    DISPATCH_INTERVAL = 1.0   # seconds between dispatch cycles

    def __init__(self, host: str = "127.0.0.1", port: int = 9999,
                 db_path: str = "broker.db"):
        self.host = host
        self.port = port
        self.db = MessageDatabase(db_path)
        self.guarantor = DeliveryGuarantor(self.db)
        self.guarantor.set_send_callback(self._send_to_consumer)

        # consumer_id → socket
        self._consumers: Dict[str, socket.socket] = {}
        self._consumers_lock = threading.Lock()

        self._server_sock: Optional[socket.socket] = None
        self._running = False

    # ─────────────────────────── Lifecycle ────────────────────────────────

    def start(self):
        self._running = True
        self.guarantor.start()

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(50)
        logger.info("Broker listening on %s:%d", self.host, self.port)

        dispatcher = threading.Thread(
            target=self._dispatch_loop, daemon=True, name="dispatcher"
        )
        dispatcher.start()

        try:
            while self._running:
                try:
                    conn, addr = self._server_sock.accept()
                    logger.info("New connection from %s", addr)
                    t = threading.Thread(
                        target=self._handle_client,
                        args=(conn, addr),
                        daemon=True
                    )
                    t.start()
                except OSError:
                    break
        finally:
            self.stop()

    def stop(self):
        self._running = False
        self.guarantor.stop()
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        logger.info("Broker stopped")

    # ─────────────────────────── Client handler ───────────────────────────

    def _handle_client(self, conn: socket.socket, addr):
        buffer = ""
        client_consumer_id: Optional[str] = None

        try:
            while self._running:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8", errors="replace")

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        result = self._process_command(msg, conn)
                        if result.get("consumer_id"):
                            client_consumer_id = result["consumer_id"]
                        resp = json.dumps(result) + "\n"
                        conn.sendall(resp.encode())
                    except json.JSONDecodeError as e:
                        err = json.dumps({"status": "error",
                                          "reason": f"invalid JSON: {e}"}) + "\n"
                        conn.sendall(err.encode())
        except (ConnectionResetError, BrokenPipeError):
            logger.info("Client %s disconnected", addr)
        finally:
            conn.close()
            if client_consumer_id:
                with self._consumers_lock:
                    self._consumers.pop(client_consumer_id, None)
            logger.info("Connection closed: %s", addr)

    def _process_command(self, msg: dict, conn: socket.socket) -> dict:
        cmd = msg.get("cmd", "")

        if cmd == "publish":
            return self._cmd_publish(msg)

        elif cmd == "subscribe":
            return self._cmd_subscribe(msg, conn)

        elif cmd == "unsubscribe":
            return self._cmd_unsubscribe(msg)

        elif cmd == "ack":
            return self._cmd_ack(msg)

        elif cmd == "nack":
            return self._cmd_nack(msg)

        elif cmd == "stats":
            return self._cmd_stats()

        else:
            return {"status": "error", "reason": f"unknown command: {cmd}"}

    # ─────────────────────────── Commands ─────────────────────────────────

    def _cmd_publish(self, msg: dict) -> dict:
        topic   = msg.get("topic", "")
        payload = msg.get("payload", "")
        mode    = msg.get("mode", DeliveryMode.AT_LEAST_ONCE)
        producer_id = msg.get("producer_id", "anon")

        if not topic or not payload:
            return {"status": "error", "reason": "topic and payload required"}

        # Validate delivery mode
        try:
            DeliveryMode(mode)
        except ValueError:
            mode = DeliveryMode.AT_LEAST_ONCE

        message_id = self.db.save_message(
            topic=topic,
            payload=payload,
            producer_id=producer_id,
            delivery_mode=mode,
        )
        logger.info("Published msg=%d topic=%s mode=%s", message_id, topic, mode)
        return {"status": "ok", "message_id": message_id}

    def _cmd_subscribe(self, msg: dict, conn: socket.socket) -> dict:
        topic       = msg.get("topic", "")
        consumer_id = msg.get("consumer_id", "")
        if not topic or not consumer_id:
            return {"status": "error", "reason": "topic and consumer_id required"}

        self.db.add_subscription(topic, consumer_id)
        with self._consumers_lock:
            self._consumers[consumer_id] = conn
        logger.info("Subscribed consumer=%s topic=%s", consumer_id, topic)
        return {"status": "ok", "consumer_id": consumer_id,
                "subscribed_to": topic}

    def _cmd_unsubscribe(self, msg: dict) -> dict:
        topic       = msg.get("topic", "")
        consumer_id = msg.get("consumer_id", "")
        self.db.remove_subscription(topic, consumer_id)
        return {"status": "ok"}

    def _cmd_ack(self, msg: dict) -> dict:
        message_id  = msg.get("message_id")
        consumer_id = msg.get("consumer_id", "")
        if message_id is None or not consumer_id:
            return {"status": "error", "reason": "message_id and consumer_id required"}
        self.guarantor.on_ack(int(message_id), consumer_id)
        return {"status": "ok"}

    def _cmd_nack(self, msg: dict) -> dict:
        message_id  = msg.get("message_id")
        consumer_id = msg.get("consumer_id", "")
        error       = msg.get("error", "")
        if message_id is None or not consumer_id:
            return {"status": "error", "reason": "message_id and consumer_id required"}
        self.guarantor.on_nack(int(message_id), consumer_id, error)
        return {"status": "ok"}

    def _cmd_stats(self) -> dict:
        stats = self.db.get_stats()
        stats.update(self.guarantor.get_status())
        return {"status": "ok", "stats": stats}

    # ─────────────────────────── Dispatcher ───────────────────────────────

    def _dispatch_loop(self):
        """Continuously poll DB for pending messages and push to consumers."""
        logger.info("Dispatcher started")
        while self._running:
            time.sleep(self.DISPATCH_INTERVAL)
            try:
                self._dispatch_pending()
            except Exception as e:
                logger.exception("Dispatch error: %s", e)

    def _dispatch_pending(self):
        """For each subscribed topic, send pending messages to consumers."""
        # Collect active topics
        with self._consumers_lock:
            active_consumers = dict(self._consumers)

        if not active_consumers:
            return

        # Get unique topics from DB
        conn_db = self.db._get_conn()
        rows = conn_db.execute(
            "SELECT DISTINCT topic FROM subscriptions"
        ).fetchall()

        for row in rows:
            topic = row["topic"]
            subscribers = self.db.get_subscribers(topic)
            pending = self.db.get_pending_messages(topic)

            for message in pending:
                for consumer_id in subscribers:
                    sock = active_consumers.get(consumer_id)
                    if sock is None:
                        continue  # consumer offline

                    mode = DeliveryMode(message["delivery_mode"])
                    dedup_key = f"{message['id']}:{consumer_id}"

                    proceed = self.guarantor.prepare_delivery(
                        message["id"], consumer_id, mode, dedup_key
                    )
                    if not proceed:
                        continue

                    self.db.mark_delivered(message["id"])
                    self._send_to_consumer(sock, message, consumer_id)

    def _send_to_consumer(self, sock, message: dict, consumer_id: str):
        """Send a message frame to a consumer socket."""
        frame = {
            "type":       "message",
            "message_id": message["id"],
            "topic":      message["topic"],
            "payload":    message["payload"],
            "mode":       message["delivery_mode"],
            "consumer_id": consumer_id,
        }
        try:
            sock.sendall((json.dumps(frame) + "\n").encode())
            self.db.log_delivery(message["id"], consumer_id, "sent")
            logger.info("Sent msg=%d → consumer=%s", message["id"], consumer_id)
        except (BrokenPipeError, OSError) as e:
            logger.warning("Failed to send msg=%d to %s: %s",
                           message["id"], consumer_id, e)
            self.guarantor.on_nack(message["id"], consumer_id, str(e))
