"""Consume clickstream records and atomically maintain Redis analytics."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis
from redis.exceptions import RedisError


EVENT_TYPES = frozenset({"page_view", "product_view", "add_to_cart", "purchase", "search"})


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    value = os.getenv(name, str(default))
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer; got {value!r}") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream.events")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", f"{KAFKA_TOPIC}.dead-letter")
KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "clickstream-analytics-v1")
KAFKA_TOPIC_PARTITIONS = _env_int("KAFKA_TOPIC_PARTITIONS", 6)
KAFKA_REPLICATION_FACTOR = _env_int("KAFKA_REPLICATION_FACTOR", 1)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
KEY_PREFIX = os.getenv("ANALYTICS_KEY_PREFIX", "analytics").strip(":") or "analytics"
BUCKET_SECONDS = _env_int("BUCKET_SECONDS", 10)
ACTIVE_USER_WINDOW_SECONDS = _env_int("ACTIVE_USER_WINDOW_SECONDS", 60)
BUCKET_RETENTION_SECONDS = _env_int("BUCKET_RETENTION_SECONDS", 7200)
DEDUP_TTL_SECONDS = _env_int("DEDUP_TTL_SECONDS", 86400)
MAX_MESSAGE_BYTES = _env_int("MAX_MESSAGE_BYTES", 65536)
CURRENCY = os.getenv("CURRENCY", "USD").upper()
HEARTBEAT_INTERVAL_SECONDS = _env_int("PROCESSOR_HEARTBEAT_INTERVAL_SECONDS", 5)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if len(CURRENCY) != 3 or not CURRENCY.isascii() or not CURRENCY.isalpha():
    raise ValueError("CURRENCY must be a three-letter currency code")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("analytics_processor")


class InvalidEvent(ValueError):
    """An event is malformed or violates the supported clickstream contract."""


@dataclass(frozen=True)
class AnalyticsEvent:
    event_id: str
    user_id: str
    event_type: str
    revenue_cents: int


def _bounded_string(value: Any, name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise InvalidEvent(f"{name} must be a string")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise InvalidEvent(f"{name} must not contain control characters")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise InvalidEvent(f"{name} must be valid UTF-8") from exc
    value = value.strip()
    if not value or len(value) > maximum:
        raise InvalidEvent(f"{name} must contain 1..{maximum} printable characters")
    return value


def parse_event(raw: bytes | str | dict[str, Any], max_bytes: int = MAX_MESSAGE_BYTES) -> AnalyticsEvent:
    """Decode and validate one JSON event without trusting producer input."""
    if isinstance(raw, bytes):
        if len(raw) > max_bytes:
            raise InvalidEvent(f"message exceeds the {max_bytes}-byte limit")
        try:
            decoded = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise InvalidEvent("message is not valid UTF-8") from exc
        try:
            payload = json.loads(decoded)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise InvalidEvent("message is not valid JSON") from exc
    elif isinstance(raw, str):
        try:
            raw_size = len(raw.encode("utf-8", errors="strict"))
        except UnicodeEncodeError as exc:
            raise InvalidEvent("message is not valid UTF-8") from exc
        if raw_size > max_bytes:
            raise InvalidEvent(f"message exceeds the {max_bytes}-byte limit")
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise InvalidEvent("message is not valid JSON") from exc
    elif isinstance(raw, dict):
        try:
            encoded = json.dumps(raw, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError, RecursionError) as exc:
            raise InvalidEvent("event cannot be serialized as JSON") from exc
        if len(encoded) > max_bytes:
            raise InvalidEvent(f"message exceeds the {max_bytes}-byte limit")
        payload = raw
    else:
        raise InvalidEvent("message must be a JSON object")

    if not isinstance(payload, dict):
        raise InvalidEvent("top-level JSON value must be an object")

    event_id = _bounded_string(payload.get("event_id"), "event_id", 128)
    user_id = _bounded_string(payload.get("user_id"), "user_id", 128)
    event_type = _bounded_string(payload.get("event_type"), "event_type", 32).lower()
    if event_type not in EVENT_TYPES:
        raise InvalidEvent(f"unsupported event_type {event_type!r}")

    currency = payload.get("currency", CURRENCY)
    if not isinstance(currency, str) or currency.upper() != CURRENCY:
        raise InvalidEvent(f"currency must be {CURRENCY}")

    amount = payload.get("amount", 0)
    if isinstance(amount, bool) or not isinstance(amount, (int, float, str, Decimal)):
        raise InvalidEvent("amount must be a decimal number")
    try:
        decimal_amount = Decimal(str(amount))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidEvent("amount must be a decimal number") from exc
    if not decimal_amount.is_finite() or decimal_amount < 0 or decimal_amount > Decimal("100000000"):
        raise InvalidEvent("amount must be finite and between 0 and 100000000")

    try:
        quantized_amount = decimal_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise InvalidEvent("amount has unsupported precision") from exc
    revenue_cents = int(quantized_amount * 100) if event_type == "purchase" else 0
    return AnalyticsEvent(
        event_id=event_id,
        user_id=user_id,
        event_type=event_type,
        revenue_cents=revenue_cents,
    )


# A single Redis script performs deduplication, lifetime counters, time-bucket
# counters, unique-user tracking, and active-user expiry without interleaving.
REDIS_ATOMIC_UPDATE_SCRIPT = r"""
if redis.call('EXISTS', KEYS[1]) == 1 then
    return {0, 0, 0, 0}
end

local event_type = ARGV[1]
local revenue_cents = tonumber(ARGV[2])
local user_id = ARGV[3]
local now_ms = tonumber(ARGV[4])
local bucket_start_ms = tonumber(ARGV[5])
local bucket_retention = tonumber(ARGV[6])
local active_window_ms = tonumber(ARGV[7])
local dedupe_ttl = tonumber(ARGV[8])

local fields = {
    page_view = 'page_views',
    product_view = 'product_views',
    add_to_cart = 'add_to_carts',
    purchase = 'purchases',
    search = 'searches'
}

redis.call('HINCRBY', KEYS[2], 'events_total', 1)
redis.call('HINCRBY', KEYS[3], 'events', 1)
redis.call('HSET', KEYS[3], 'timestamp_ms', bucket_start_ms)

local metric_field = fields[event_type]
if metric_field then
    redis.call('HINCRBY', KEYS[2], metric_field, 1)
    redis.call('HINCRBY', KEYS[3], metric_field, 1)
end
if event_type == 'purchase' and revenue_cents > 0 then
    redis.call('HINCRBY', KEYS[2], 'revenue_cents', revenue_cents)
    redis.call('HINCRBY', KEYS[3], 'revenue_cents', revenue_cents)
end

local bucket_user_added = redis.call('SADD', KEYS[4], user_id)
if bucket_user_added == 1 then
    redis.call('HINCRBY', KEYS[3], 'unique_users', 1)
end

redis.call('ZADD', KEYS[5], now_ms, user_id)
redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', now_ms - active_window_ms)
local active_users = redis.call('ZCARD', KEYS[5])
redis.call('PFADD', KEYS[6], user_id)
local lifetime_unique_users = redis.call('PFCOUNT', KEYS[6])
redis.call('HSET', KEYS[2], 'active_users', active_users,
    'unique_users', lifetime_unique_users, 'updated_at_ms', now_ms)
redis.call('HSET', KEYS[3], 'active_users', active_users)

redis.call('EXPIRE', KEYS[3], bucket_retention)
redis.call('EXPIRE', KEYS[4], bucket_retention)
redis.call('EXPIRE', KEYS[5], math.max(1, math.ceil(active_window_ms / 1000) * 2))
redis.call('SET', KEYS[1], '1', 'EX', dedupe_ttl)
return {1, active_users, lifetime_unique_users, bucket_user_added}
"""


class AnalyticsProcessor:
    """Kafka consumer with bounded retries and idempotent Redis side effects."""

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.redis: Redis = Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=5,
            health_check_interval=30,
            retry_on_timeout=True,
        )
        self.redis_update = self.redis.register_script(REDIS_ATOMIC_UPDATE_SCRIPT)
        self.admin = AdminClient(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "clickstream-topic-admin",
            }
        )
        self.consumer: Consumer | None = None
        self.dead_letter_producer: Producer | None = None
        self.heartbeat_key = f"{KEY_PREFIX}:processor:heartbeat"
        self._last_heartbeat = 0.0

    def wait_for_dependencies(self) -> bool:
        """Wait for Kafka and Redis to recover rather than exiting on startup races."""
        delay = 1.0
        while not self.stop_event.is_set():
            try:
                self.redis.ping()
                self.admin.list_topics(timeout=5)
                return True
            except (RedisError, KafkaException, OSError, TimeoutError) as exc:
                logger.warning("Dependencies not ready (%s); retrying in %.1fs", exc, delay)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 30.0)
            except Exception:
                logger.exception("Unexpected error checking dependencies; retrying in %.1fs", delay)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 30.0)
        return False

    def ensure_topics(self) -> None:
        """Create the event and dead-letter topics if they are not present."""
        topic_names = {KAFKA_TOPIC, KAFKA_DLQ_TOPIC}
        new_topics = [
            NewTopic(
                name,
                num_partitions=KAFKA_TOPIC_PARTITIONS,
                replication_factor=KAFKA_REPLICATION_FACTOR,
            )
            for name in sorted(topic_names)
        ]
        futures = self.admin.create_topics(new_topics, request_timeout=15)
        for topic_name, future in futures.items():
            try:
                future.result(timeout=20)
                logger.info("Created Kafka topic %s", topic_name)
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                error_code = error.code() if error is not None and hasattr(error, "code") else None
                if error_code == KafkaError.TOPIC_ALREADY_EXISTS:
                    continue
                raise

    def _new_consumer(self) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "client.id": "clickstream-analytics-processor",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "session.timeout.ms": 45000,
                "heartbeat.interval.ms": 3000,
                "max.poll.interval.ms": 300000,
                "allow.auto.create.topics": False,
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        consumer.subscribe([KAFKA_TOPIC])
        return consumer

    def _new_dead_letter_producer(self) -> Producer:
        return Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "clickstream-dead-letter-producer",
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": 30000,
                "compression.type": "zstd",
            }
        )

    def _touch_heartbeat(self, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - self._last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
            try:
                self.redis.set(self.heartbeat_key, str(time.time()), ex=30)
                self._last_heartbeat = now
            except RedisError:
                logger.warning("Could not refresh processor heartbeat; Redis may be unavailable", exc_info=True)

    def _apply_event(self, event: AnalyticsEvent) -> tuple[int, ...]:
        now_ms = int(time.time() * 1000)
        bucket_start_ms = (now_ms // (BUCKET_SECONDS * 1000)) * BUCKET_SECONDS * 1000
        event_digest = hashlib.sha256(event.event_id.encode("utf-8")).hexdigest()
        bucket_number = bucket_start_ms // 1000
        keys = [
            f"{KEY_PREFIX}:dedupe:{event_digest}",
            f"{KEY_PREFIX}:totals",
            f"{KEY_PREFIX}:bucket:{bucket_number}",
            f"{KEY_PREFIX}:bucket:{bucket_number}:users",
            f"{KEY_PREFIX}:active_users",
            f"{KEY_PREFIX}:unique_users",
        ]
        result = self.redis_update(
            keys=keys,
            args=[
                event.event_type,
                event.revenue_cents,
                event.user_id,
                now_ms,
                bucket_start_ms,
                BUCKET_RETENTION_SECONDS,
                ACTIVE_USER_WINDOW_SECONDS * 1000,
                DEDUP_TTL_SECONDS,
            ],
        )
        return tuple(int(value) for value in result)

    def _send_to_dead_letter(self, message: Any, reason: str) -> None:
        if self.dead_letter_producer is None:
            raise RuntimeError("dead-letter producer is not initialized")

        raw_value = message.value() or b""
        truncated_value = raw_value[:MAX_MESSAGE_BYTES]
        record = {
            "source_topic": message.topic(),
            "source_partition": message.partition(),
            "source_offset": message.offset(),
            "failed_at_ms": int(time.time() * 1000),
            "reason": reason[:1000],
            "raw_prefix_base64": base64.b64encode(truncated_value).decode("ascii"),
            "raw_truncated": len(raw_value) > len(truncated_value),
        }
        delivered: dict[str, Any] = {"error": None}

        def on_delivery(error: Any, _record: Any) -> None:
            delivered["error"] = error

        value = json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        key = message.key()
        deadline = time.monotonic() + 30
        while not self.stop_event.is_set() and time.monotonic() < deadline:
            try:
                self.dead_letter_producer.produce(
                    KAFKA_DLQ_TOPIC,
                    key=key,
                    value=value,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                self.dead_letter_producer.poll(0.1)
            except KafkaException:
                logger.warning("Kafka rejected a DLQ publish; retrying", exc_info=True)
                self.dead_letter_producer.poll(0.2)
                self.stop_event.wait(0.2)
        else:
            raise RuntimeError("could not enqueue dead-letter record before timeout")

        remaining = self.dead_letter_producer.flush(timeout=30)
        if remaining:
            raise RuntimeError(f"{remaining} dead-letter message(s) remain undelivered")
        if delivered["error"] is not None:
            raise RuntimeError(f"dead-letter delivery failed: {delivered['error']}")
        logger.warning(
            "Sent invalid event to %s (partition=%s offset=%s): %s",
            KAFKA_DLQ_TOPIC,
            message.partition(),
            message.offset(),
            reason,
        )

    def _seek_for_retry(self, message: Any) -> bool:
        try:
            self.consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
            return True
        except (KafkaException, AttributeError):
            logger.exception("Could not seek back to failed Kafka offset")
            return False

    def _commit(self, synchronous: bool = True) -> None:
        if self.consumer is None:
            return
        try:
            self.consumer.commit(asynchronous=not synchronous)
        except KafkaException:
            logger.warning("Kafka offset commit failed; processed records may be replayed", exc_info=True)

    def _process_loop(self) -> None:
        self.consumer = self._new_consumer()
        self.dead_letter_producer = self._new_dead_letter_producer()
        logger.info(
            "Consuming topic=%s group=%s; Redis buckets=%ss, active-user window=%ss",
            KAFKA_TOPIC,
            KAFKA_GROUP_ID,
            BUCKET_SECONDS,
            ACTIVE_USER_WINDOW_SECONDS,
        )

        processed_since_commit = 0
        last_commit = time.monotonic()
        self._touch_heartbeat(force=True)

        while not self.stop_event.is_set():
            self._touch_heartbeat()
            try:
                message = self.consumer.poll(timeout=1.0)
            except KafkaException:
                logger.warning("Kafka poll failed; waiting for the client to reconnect", exc_info=True)
                self.stop_event.wait(1.0)
                continue

            if message is None:
                if processed_since_commit and time.monotonic() - last_commit >= 1.0:
                    self._commit(synchronous=True)
                    processed_since_commit = 0
                    last_commit = time.monotonic()
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.warning("Kafka consumer reported an error: %s", message.error())
                continue

            try:
                event = parse_event(message.value())
            except InvalidEvent as exc:
                try:
                    self._send_to_dead_letter(message, str(exc))
                except (RuntimeError, KafkaException, OSError) as dlq_error:
                    logger.error("DLQ publish failed; retrying source offset: %s", dlq_error, exc_info=True)
                    if not self._seek_for_retry(message):
                        raise RuntimeError("unable to rewind source offset after DLQ failure")
                    self.stop_event.wait(1.0)
                    continue
            else:
                try:
                    result = self._apply_event(event)
                    if result and result[0] == 0:
                        logger.debug("Ignored duplicate event_id=%s", event.event_id)
                except RedisError:
                    logger.warning(
                        "Redis update failed for topic=%s partition=%s offset=%s; retrying",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        exc_info=True,
                    )
                    if not self._seek_for_retry(message):
                        raise RuntimeError("unable to rewind source offset after Redis failure")
                    self.stop_event.wait(1.0)
                    continue

            processed_since_commit += 1
            now = time.monotonic()
            if processed_since_commit >= 200 or now - last_commit >= 1.0:
                self._commit(synchronous=True)
                processed_since_commit = 0
                last_commit = now

        if processed_since_commit:
            self._commit(synchronous=True)

    def run(self) -> None:
        retry_delay = 1.0
        while not self.stop_event.is_set():
            try:
                if not self.wait_for_dependencies():
                    break
                self.ensure_topics()
                retry_delay = 1.0
                self._process_loop()
            except (KafkaException, RedisError, OSError, RuntimeError):
                logger.exception("Processor loop failed; rebuilding clients in %.1fs", retry_delay)
                self.stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, 30.0)
            except Exception:
                logger.exception("Unexpected processor failure; restarting loop in %.1fs", retry_delay)
                self.stop_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, 30.0)
            finally:
                if self.consumer is not None:
                    try:
                        self.consumer.close()
                    except Exception:
                        logger.warning("Error while closing Kafka consumer", exc_info=True)
                    self.consumer = None
                if self.dead_letter_producer is not None:
                    try:
                        pending = self.dead_letter_producer.flush(timeout=5)
                        if pending:
                            logger.warning("%d DLQ message(s) not flushed during shutdown", pending)
                    except Exception:
                        logger.warning("Error while flushing DLQ producer", exc_info=True)
                    self.dead_letter_producer = None
        try:
            self.redis.delete(self.heartbeat_key)
        except RedisError:
            logger.warning("Could not clear processor heartbeat during shutdown", exc_info=True)
        try:
            self.redis.close()
        except Exception:
            logger.warning("Error while closing Redis connection", exc_info=True)


def main() -> int:
    processor = AnalyticsProcessor()

    def request_shutdown(signum: int, _frame: Any) -> None:
        logger.info("Received signal %s; shutting down cleanly", signum)
        processor.stop_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    try:
        processor.run()
    except KeyboardInterrupt:
        processor.stop_event.set()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        logger.exception("Analytics processor exited after an unrecoverable error")
        raise
