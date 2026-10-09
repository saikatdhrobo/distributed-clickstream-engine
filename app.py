"""Flask REST API and Flask-SocketIO realtime dashboard server."""

from __future__ import annotations

import os

# Patch before importing Flask/Redis so blocking I/O yields correctly when the
# Flask-SocketIO server is run with gevent (the production Compose default).
ASYNC_MODE = os.getenv("SOCKETIO_ASYNC_MODE", "gevent").strip().lower()
if ASYNC_MODE == "gevent":
    from gevent import monkey

    monkey.patch_all()

import logging
import math
import time
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
from redis import Redis
from redis.exceptions import RedisError


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    value = os.getenv(name, str(default))
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer; got {value!r}") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


def _env_float(name: str, default: float, minimum: float = 0.1) -> float:
    value = os.getenv(name, str(default))
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number; got {value!r}") from exc
    if not math.isfinite(parsed) or parsed < minimum:
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return parsed


BASE_DIR = Path(__file__).resolve().parent
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
KEY_PREFIX = os.getenv("ANALYTICS_KEY_PREFIX", "analytics").strip(":") or "analytics"
BUCKET_SECONDS = _env_int("BUCKET_SECONDS", 10)
SERIES_BUCKETS = _env_int("SERIES_BUCKETS", 60)
ACTIVE_USER_WINDOW_SECONDS = _env_int("ACTIVE_USER_WINDOW_SECONDS", 60)
BROADCAST_INTERVAL_SECONDS = _env_float("BROADCAST_INTERVAL_SECONDS", 1.0)
CURRENCY = os.getenv("CURRENCY", "USD").upper()
if len(CURRENCY) != 3 or not CURRENCY.isascii() or not CURRENCY.isalpha():
    raise ValueError("CURRENCY must be a three-letter ASCII currency code")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HEARTBEAT_MAX_AGE_SECONDS = _env_int("PROCESSOR_HEARTBEAT_MAX_AGE_SECONDS", 30)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("clickstream_api")

app = Flask(__name__)
app.config.update(JSON_SORT_KEYS=False, MAX_CONTENT_LENGTH=1024 * 1024)

allowed_origin_value = os.getenv("ALLOWED_ORIGINS", "").strip()
if allowed_origin_value == "*":
    allowed_origins: str | list[str] | None = "*"
elif allowed_origin_value:
    allowed_origins = [origin.strip() for origin in allowed_origin_value.split(",") if origin.strip()]
else:
    # Same-origin is the safe default; the browser dashboard needs no CORS.
    allowed_origins = None

socketio = SocketIO(
    app,
    async_mode=ASYNC_MODE,
    cors_allowed_origins=allowed_origins,
    ping_interval=25,
    ping_timeout=20,
    max_http_buffer_size=1_000_000,
    logger=False,
    engineio_logger=False,
)

redis_client: Redis = Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=3,
    health_check_interval=30,
    retry_on_timeout=True,
)

# Active users are a sliding window, so their count must decay even when no new
# Kafka events arrive. Recalculate and write the count atomically on each snapshot.
ACTIVE_USERS_SNAPSHOT_SCRIPT = r"""
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms - window_ms)
local active_users = redis.call('ZCARD', KEYS[1])
redis.call('HSET', KEYS[2], 'active_users', active_users)
return active_users
"""
active_users_snapshot = redis_client.register_script(ACTIVE_USERS_SNAPSHOT_SCRIPT)

SERIES_FIELDS = (
    "events",
    "page_views",
    "product_views",
    "add_to_carts",
    "purchases",
    "searches",
    "revenue_cents",
    "unique_users",
    "active_users",
)
TOTAL_FIELDS = (
    "events_total",
    "page_views",
    "product_views",
    "add_to_carts",
    "purchases",
    "searches",
    "revenue_cents",
    "unique_users",
    "active_users",
    "updated_at_ms",
)


def _int_value(mapping: dict[str, Any], field: str, default: int = 0) -> int:
    try:
        return int(mapping.get(field, default))
    except (TypeError, ValueError, OverflowError):
        return default


def build_metrics_snapshot() -> dict[str, Any]:
    """Read a fixed number of current processing-time buckets from Redis."""
    now_ms = int(time.time() * 1000)
    current_bucket_seconds = (now_ms // 1000 // BUCKET_SECONDS) * BUCKET_SECONDS
    bucket_seconds = [
        current_bucket_seconds - (SERIES_BUCKETS - index - 1) * BUCKET_SECONDS
        for index in range(SERIES_BUCKETS)
    ]
    bucket_keys = [f"{KEY_PREFIX}:bucket:{bucket_seconds_value}" for bucket_seconds_value in bucket_seconds]

    active_users = int(
        active_users_snapshot(
            keys=[f"{KEY_PREFIX}:active_users", f"{KEY_PREFIX}:totals"],
            args=[now_ms, ACTIVE_USER_WINDOW_SECONDS * 1000],
        )
    )
    pipeline = redis_client.pipeline(transaction=False)
    pipeline.hgetall(f"{KEY_PREFIX}:totals")
    for key in bucket_keys:
        pipeline.hgetall(key)
    rows = pipeline.execute()
    totals_raw = rows[0] if rows else {}

    totals = {field: _int_value(totals_raw, field) for field in TOTAL_FIELDS}
    totals["active_users"] = active_users
    series: list[dict[str, int]] = []
    for bucket_start, row in zip(bucket_seconds, rows[1:]):
        point = {field: _int_value(row, field) for field in SERIES_FIELDS}
        point["timestamp_ms"] = _int_value(row, "timestamp_ms", bucket_start * 1000)
        # Empty buckets still carry the expected time, so charts never shift.
        if not point["timestamp_ms"]:
            point["timestamp_ms"] = bucket_start * 1000
        series.append(point)

    return {
        "status": "ok",
        "server_time_ms": now_ms,
        "bucket_seconds": BUCKET_SECONDS,
        "active_user_window_seconds": ACTIVE_USER_WINDOW_SECONDS,
        "currency": CURRENCY,
        "totals": totals,
        "series": series,
    }


def _processor_heartbeat_age() -> float | None:
    raw = redis_client.get(f"{KEY_PREFIX}:processor:heartbeat")
    if raw is None:
        return None
    try:
        return max(0.0, time.time() - float(raw))
    except (TypeError, ValueError):
        return None


@app.get("/")
def index() -> Any:
    response = send_from_directory(BASE_DIR, "index.html", max_age=0)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/app.js")
def frontend_javascript() -> Any:
    return send_from_directory(BASE_DIR, "app.js", mimetype="application/javascript", max_age=60)


@app.get("/api/metrics")
def metrics() -> Any:
    try:
        return jsonify(build_metrics_snapshot())
    except RedisError:
        logger.warning("Metrics API could not reach Redis", exc_info=True)
        return jsonify({"status": "unavailable", "error": "Analytics store is temporarily unavailable"}), 503
    except Exception:
        logger.exception("Metrics API failed unexpectedly")
        return jsonify({"status": "error", "error": "Unable to build metrics snapshot"}), 500


@app.get("/health/live")
def live() -> Any:
    return jsonify({"status": "live"})


@app.get("/health/ready")
def ready() -> Any:
    try:
        redis_client.ping()
        heartbeat_age = _processor_heartbeat_age()
        if heartbeat_age is None or heartbeat_age > HEARTBEAT_MAX_AGE_SECONDS:
            return (
                jsonify(
                    {
                        "status": "not_ready",
                        "reason": "analytics processor heartbeat is missing or stale",
                        "processor_heartbeat_age_seconds": heartbeat_age,
                    }
                ),
                503,
            )
        return jsonify({"status": "ready", "processor_heartbeat_age_seconds": round(heartbeat_age, 2)})
    except RedisError:
        logger.warning("Readiness check could not reach Redis", exc_info=True)
        return jsonify({"status": "not_ready", "reason": "Redis is unavailable"}), 503


@socketio.on("connect")
def handle_connect(auth: Any = None) -> None:
    logger.debug("Dashboard WebSocket connected")
    try:
        emit("metrics", build_metrics_snapshot())
    except RedisError:
        logger.warning("Could not send initial WebSocket snapshot", exc_info=True)


@socketio.on("disconnect")
def handle_disconnect(reason: Any = None) -> None:
    logger.debug("Dashboard WebSocket disconnected: %s", reason)


def broadcast_metrics() -> None:
    """Continuously broadcast snapshots; transient Redis errors never kill the task."""
    logger.info("Starting metrics broadcaster, interval=%.2fs", BROADCAST_INTERVAL_SECONDS)
    retry_delay = BROADCAST_INTERVAL_SECONDS
    while True:
        try:
            snapshot = build_metrics_snapshot()
            socketio.emit("metrics", snapshot, namespace="/")
            retry_delay = BROADCAST_INTERVAL_SECONDS
            socketio.sleep(BROADCAST_INTERVAL_SECONDS)
        except RedisError:
            logger.warning("Metrics broadcaster cannot reach Redis; will retry", exc_info=True)
            socketio.sleep(min(max(retry_delay, 1.0), 30.0))
            retry_delay = min(max(retry_delay * 2, 1.0), 30.0)
        except Exception:
            logger.exception("Unexpected broadcaster error; keeping the task alive")
            socketio.sleep(min(max(retry_delay, 1.0), 30.0))
            retry_delay = min(max(retry_delay * 2, 1.0), 30.0)


_broadcaster_started = False


def start_background_tasks() -> None:
    global _broadcaster_started
    if _broadcaster_started or os.getenv("START_BROADCASTER", "true").lower() not in {"1", "true", "yes"}:
        return
    socketio.start_background_task(broadcast_metrics)
    _broadcaster_started = True


@app.after_request
def set_security_headers(response: Any) -> Any:
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "script-src 'self' https://cdn.tailwindcss.com https://cdn.jsdelivr.net 'unsafe-eval'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self' data:; "
        "connect-src 'self' ws: wss:; "
        "object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
    )
    if response.is_json:
        response.headers.setdefault("Cache-Control", "no-store")
    return response


start_background_tasks()


if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=_env_int("APP_PORT", 5000),
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )
