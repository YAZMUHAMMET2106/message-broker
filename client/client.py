"""
Broker Client Library
=====================
Provides Producer and Consumer classes for connecting to the broker.
"""

import json
import logging
import socket
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class BrokerClient:
    """Base TCP client for the message broker."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9999,
                 timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    def connect(self):
        self._sock = socket.create_connection((self.host, self.port),
                                              timeout=self.timeout)
        logger.info("Connected to broker at %s:%d", self.host, self.port)

    def disconnect(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def _send(self, data: dict) -> dict:
        with self._lock:
            payload = (json.dumps(data) + "\n").encode()
            self._sock.sendall(payload)
            response = b""
            while b"\n" not in response:
                chunk = self._sock.recv(4096)
                if not chunk:
                    raise ConnectionError("Broker closed connection")
                response += chunk
            return json.loads(response.split(b"\n")[0])


class Producer(BrokerClient):
    """
    Message producer.

    Example
    -------
    >>> p = Producer()
    >>> p.connect()
    >>> msg_id = p.publish("orders", '{"item": "book"}',
    ...                    mode="at_least_once")
    >>> p.disconnect()
    """

    def publish(self, topic: str, payload: str,
                mode: str = "at_least_once",
                producer_id: str = "producer-1") -> int:
        """
        Publish a message to a topic.

        Parameters
        ----------
        topic       : destination topic
        payload     : message body (string / JSON string)
        mode        : delivery guarantee mode
                      'at_most_once' | 'at_least_once' | 'exactly_once'
        producer_id : identifier for this producer

        Returns
        -------
        message_id assigned by the broker
        """
        resp = self._send({
            "cmd":         "publish",
            "topic":       topic,
            "payload":     payload,
            "mode":        mode,
            "producer_id": producer_id,
        })
        if resp.get("status") != "ok":
            raise RuntimeError(f"Publish failed: {resp.get('reason')}")
        msg_id = resp["message_id"]
        logger.info("Published msg=%d topic=%s mode=%s", msg_id, topic, mode)
        return msg_id

    def get_stats(self) -> dict:
        resp = self._send({"cmd": "stats"})
        return resp.get("stats", {})


class Consumer(BrokerClient):
    """
    Message consumer with automatic ACK/NACK handling.

    Example
    -------
    >>> def handler(msg_id, topic, payload):
    ...     print(f"Got: {payload}")
    ...     return True   # True → ACK, False → NACK
    ...
    >>> c = Consumer(consumer_id="worker-1")
    >>> c.connect()
    >>> c.subscribe("orders", handler)
    >>> c.listen()          # blocking
    """

    def __init__(self, consumer_id: str, **kwargs):
        super().__init__(**kwargs)
        self.consumer_id = consumer_id
        self._handlers: dict[str, Callable] = {}
        self._running = False

    def subscribe(self, topic: str, handler: Callable):
        """Subscribe to a topic with a message handler function."""
        resp = self._send({
            "cmd":         "subscribe",
            "topic":       topic,
            "consumer_id": self.consumer_id,
        })
        if resp.get("status") != "ok":
            raise RuntimeError(f"Subscribe failed: {resp.get('reason')}")
        self._handlers[topic] = handler
        logger.info("Subscribed consumer=%s topic=%s", self.consumer_id, topic)

    def unsubscribe(self, topic: str):
        self._send({
            "cmd":         "unsubscribe",
            "topic":       topic,
            "consumer_id": self.consumer_id,
        })
        self._handlers.pop(topic, None)

    def listen(self, blocking: bool = True):
        """
        Start consuming messages.

        Parameters
        ----------
        blocking : if True, block the calling thread
                   if False, start a background thread
        """
        self._running = True
        if blocking:
            self._listen_loop()
        else:
            t = threading.Thread(target=self._listen_loop, daemon=True,
                                 name=f"consumer-{self.consumer_id}")
            t.start()
            return t

    def stop(self):
        self._running = False

    # ──────────────────────── Internal ────────────────────────────────────

    def _listen_loop(self):
        logger.info("Consumer %s listening…", self.consumer_id)
        buffer = ""
        self._sock.settimeout(1.0)

        while self._running:
            try:
                data = self._sock.recv(4096)
                if not data:
                    logger.warning("Broker closed connection")
                    break
                buffer += data.decode("utf-8", errors="replace")

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        frame = json.loads(line)
                        self._handle_frame(frame)
                    except json.JSONDecodeError:
                        logger.warning("Invalid frame: %s", line[:80])

            except socket.timeout:
                continue
            except (ConnectionResetError, OSError):
                break

    def _handle_frame(self, frame: dict):
        """Dispatch an incoming message frame to the appropriate handler."""
        if frame.get("type") != "message":
            return  # skip ACK responses etc.

        msg_id   = frame["message_id"]
        topic    = frame["topic"]
        payload  = frame["payload"]
        mode     = frame.get("mode", "at_least_once")

        handler = self._handlers.get(topic)
        if handler is None:
            logger.warning("No handler for topic=%s", topic)
            return

        try:
            result = handler(msg_id, topic, payload)
            success = result is not False
        except Exception as e:
            logger.exception("Handler error: %s", e)
            success = False

        # Send ACK or NACK (not needed for at_most_once)
        if mode != "at_most_once":
            cmd = "ack" if success else "nack"
            try:
                with self._lock:
                    pkt = json.dumps({
                        "cmd":         cmd,
                        "message_id":  msg_id,
                        "consumer_id": self.consumer_id,
                    }) + "\n"
                    self._sock.sendall(pkt.encode())
                logger.debug("%s sent for msg=%d", cmd.upper(), msg_id)
            except OSError as e:
                logger.error("Failed to send %s: %s", cmd, e)
