#!/usr/bin/env python3
"""
Message Broker — Entry Point
Usage:
    python main.py [--host HOST] [--port PORT] [--db PATH]
"""

import argparse
import logging
import sys
import os

# Allow imports from project root
sys.path.insert(0, os.path.dirname(__file__))

from broker.server import BrokerServer


def setup_logging(level: str = "INFO"):
    os.makedirs("logs", exist_ok=True)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/broker.log"),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Message Broker Server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", default=9999, type=int, help="Bind port")
    parser.add_argument("--db",   default="broker.db",  help="SQLite DB path")
    parser.add_argument("--log",  default="INFO",        help="Log level")
    args = parser.parse_args()

    setup_logging(args.log)
    logger = logging.getLogger("main")

    logger.info("Starting Message Broker")
    logger.info("Host=%s  Port=%d  DB=%s", args.host, args.port, args.db)

    server = BrokerServer(host=args.host, port=args.port, db_path=args.db)
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
        server.stop()


if __name__ == "__main__":
    main()
