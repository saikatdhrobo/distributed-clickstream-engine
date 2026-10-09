"""Generate realistic synthetic e-commerce clickstream traffic into Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import signal
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import KafkaException, Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream.events")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("traffic_generator")

EVENTS = ("page_view", "product_view", "add_to_cart", "purchase", "search")
EVENT_WEIGHTS = (48, 24, 13, 5, 10)
PRODUCTS = (
    ("sku-1001", "Electronics"),
    ("sku-1002", "Electronics"),
    ("sku-2001", "Home"),
    ("sku-2002", "Home"),
    ("sku-3001", "Apparel"),
    ("sku-3002", "Apparel"),
    ("sku-4001", "Beauty"),
)


class DeliveryStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.delivered = 0
        self.failed = 0

    def report(self, error: Any, message: Any) -> None:
        with self._lock:
            if error is None:
                self.delivered += 1
            else:
                self.failed += 1
                logger.error(
                    "Kafka delivery failed topic=%s partition=%s: %s",
                    getattr(message, "topic", lambda: KAFKA_TOPIC)(),
                    getattr(message, "partition", lambda: -1)(),
                    error,
                )

    def snapshot(self) -> tuple[int, int]:
        with self._lock:
            return self.delivered, self.failed


def _positive_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError(f"{name} must be a positive integer")
    return parsed


def _nonnegative_float(value: str, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"{name} must be a non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError(f"{name} must be a finite, non-negative number")
    return parsed


def parse_args() -> argparse.Namespace:
    default_users = os.getenv("SIMULATED_USERS", "1000")
    default_threads = os.getenv("TRAFFIC_THREADS", default_users)
    default_interval = os.getenv("EVENT_INTERVAL_SECONDS", "1")
    default_duration = os.getenv("RUN_DURATION_SECONDS", "0")

    parser = argparse.ArgumentParser(description="Publish synthetic events to the configured Kafka topic.")
    parser.add_argument("--users", type=lambda v: _positive_int(v, "users"), default=default_users)
    parser.add_argument("--threads", type=lambda v: _positive_int(v, "threads"), default=default_threads)
    parser.add_argument(
        "--interval",
        type=lambda v: _nonnegative_float(v, "interval"),
        default=default_interval,
        help="target interval in seconds between events per virtual user (default: 1)",
    )
    parser.add_argument(
        "--duration",
        type=lambda v: _nonnegative_float(v, "duration"),
        default=default_duration,
        help="stop after this many seconds; 0 runs until interrupted",
    )
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.users > 100000:
        parser.error("--users must be at most 100000")
    if args.threads > 5000:
        parser.error("--threads must be at most 5000 to protect host resources")
    return args


def _build_event(user_number: int, session_id: str, rng: random.Random) -> dict[str, Any]:
    event_type = rng.choices(EVENTS, weights=EVENT_WEIGHTS, k=1)[0]
    product_id, category = rng.choice(PRODUCTS)
    now = datetime.now(timezone.utc)
    amount = f"{rng.randint(499, 24999) / 100:.2f}" if event_type == "purchase" else "0.00"

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": f"user-{user_number:06d}",
        "session_id": session_id,
        "product_id": product_id,
        "category": category,
        "page": f"/{category.lower()}/{product_id}",
        "amount": amount,
        "currency": "USD",
        "event_time_ms": int(now.timestamp() * 1000),
        "timestamp": now.isoformat(timespec="milliseconds"),
    }


def wait_for_kafka(producer: Producer, stop_event: threading.Event) -> bool:
    delay = 1.0
    while not stop_event.is_set():
        try:
            producer.list_topics(timeout=5)
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return True
        except (KafkaException, OSError, TimeoutError) as exc:
            logger.warning("Kafka is not ready (%s); retrying in %.1fs", exc, delay)
            stop_event.wait(delay)
            delay = min(delay * 2, 30.0)
        except Exception:
            logger.exception("Unexpected Kafka connection error; retrying in %.1fs", delay)
            stop_event.wait(delay)
            delay = min(delay * 2, 30.0)
    return False


def run_worker(
    worker_index: int,
    user_numbers: range,
    producer: Producer,
    stats: DeliveryStats,
    stop_event: threading.Event,
    interval_seconds: float,
) -> None:
    """One OS thread continuously impersonates its assigned virtual users."""
    rng = random.Random()
    sessions = {user_number: str(uuid.uuid4()) for user_number in user_numbers}
    assigned_count = max(1, len(sessions))
    per_user_delay = interval_seconds
    logger.debug("Traffic worker %d simulating %d user(s)", worker_index, assigned_count)

    while not stop_event.is_set():
        cycle_started = time.monotonic()
        for user_number in user_numbers:
            if stop_event.is_set():
                break
            event = _build_event(user_number, sessions[user_number], rng)
            key = event["user_id"].encode("utf-8")
            value = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            deadline = time.monotonic() + 30.0

            while not stop_event.is_set() and time.monotonic() < deadline:
                try:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=key,
                        value=value,
                        on_delivery=stats.report,
                    )
                    # Polling services delivery callbacks and frees producer queue space.
                    producer.poll(0)
                    break
                except BufferError:
                    producer.poll(0.05)
                    stop_event.wait(0.02)
                except KafkaException:
                    logger.warning("Kafka produce call failed; retrying event", exc_info=True)
                    producer.poll(0.1)
                    stop_event.wait(0.25)
            else:
                if not stop_event.is_set():
                    logger.error("Dropping one generated event after 30 seconds of producer backpressure")

        elapsed = time.monotonic() - cycle_started
        stop_event.wait(max(0.0, per_user_delay - elapsed))


def main() -> int:
    args = parse_args()
    thread_count = min(args.threads, args.users)
    stop_event = threading.Event()
    stats = DeliveryStats()

    def request_shutdown(signum: int, _frame: Any) -> None:
        logger.info("Received signal %s; stopping traffic workers", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "clickstream-traffic-generator",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 2147483647,
            "message.timeout.ms": 30000,
            "retry.backoff.ms": 100,
            "compression.type": "zstd",
            "linger.ms": 10,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 1000000,
            "queue.buffering.max.kbytes": 1048576,
        }
    )

    logger.info(
        "Starting %d worker thread(s) for %d virtual users, interval=%.3fs, duration=%s, topic=%s",
        thread_count,
        args.users,
        args.interval,
        f"{args.duration:.1f}s" if args.duration else "until interrupted",
        KAFKA_TOPIC,
    )

    try:
        if not wait_for_kafka(producer, stop_event):
            return 0

        worker_user_groups = [range(index, args.users, thread_count) for index in range(thread_count)]
        started_at = time.monotonic()
        next_stats_log = started_at + 10.0
        with ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="clickstream-user") as pool:
            futures = [
                pool.submit(
                    run_worker,
                    worker_index,
                    user_numbers,
                    producer,
                    stats,
                    stop_event,
                    args.interval,
                )
                for worker_index, user_numbers in enumerate(worker_user_groups)
            ]
            while not stop_event.wait(0.25):
                now = time.monotonic()
                if args.duration and now - started_at >= args.duration:
                    stop_event.set()
                    break
                if now >= next_stats_log:
                    delivered, failed = stats.snapshot()
                    logger.info("Delivery reports: delivered=%d failed=%d", delivered, failed)
                    next_stats_log = now + 10.0
                for future in futures:
                    if future.done():
                        exception = future.exception()
                        if exception is not None:
                            logger.error(
                                "Traffic worker crashed; stopping all workers",
                                exc_info=(type(exception), exception, exception.__traceback__),
                            )
                            stop_event.set()
                            break

        delivered, failed = stats.snapshot()
        remaining = producer.flush(timeout=30)
        if remaining:
            logger.error("Shutdown left %d Kafka message(s) undelivered", remaining)
        logger.info("Traffic generator stopped: delivered=%d failed=%d pending=%d", delivered, failed, remaining)
        return 0 if not remaining else 1
    except KeyboardInterrupt:
        stop_event.set()
        producer.flush(timeout=30)
        return 0
    except Exception:
        stop_event.set()
        logger.exception("Traffic generator stopped after an unexpected error")
        try:
            producer.flush(timeout=10)
        except Exception:
            logger.warning("Unable to flush producer after failure", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
