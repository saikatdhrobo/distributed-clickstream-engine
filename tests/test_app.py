import os
import time
import unittest

os.environ["START_BROADCASTER"] = "false"
os.environ["SOCKETIO_ASYNC_MODE"] = "threading"

import app as app_module  # noqa: E402


class FakePipeline:
    def __init__(self, redis_instance):
        self.redis_instance = redis_instance
        self.keys = []

    def hgetall(self, key):
        self.keys.append(key)
        return self

    def execute(self):
        totals = {
            "events_total": "12",
            "page_views": "7",
            "purchases": "1",
            "revenue_cents": "2599",
            "active_users": "3",
            "unique_users": "9",
            "updated_at_ms": "1700000000000",
        }
        buckets = [{} for _ in self.keys[1:]]
        if buckets:
            buckets[-1] = {"events": "2", "page_views": "1", "purchases": "1", "revenue_cents": "2599"}
        return [totals, *buckets]


class FakeRedis:
    def __init__(self):
        self.heartbeat = str(time.time())

    def pipeline(self, transaction=False):
        self.assert_non_transactional = transaction is False
        return FakePipeline(self)

    def ping(self):
        return True

    def get(self, _key):
        return self.heartbeat


class FlaskApiTests(unittest.TestCase):
    def setUp(self):
        self.original_redis = app_module.redis_client
        self.original_active_snapshot = app_module.active_users_snapshot
        self.fake_redis = FakeRedis()
        app_module.redis_client = self.fake_redis
        app_module.active_users_snapshot = lambda keys, args: 3
        app_module.app.config.update(TESTING=True)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.redis_client = self.original_redis
        app_module.active_users_snapshot = self.original_active_snapshot

    def test_live_health_endpoint(self):
        response = self.client.get("/health/live")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "live")

    def test_ready_health_requires_fresh_processor_heartbeat(self):
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "ready")

        self.fake_redis.heartbeat = str(time.time() - app_module.HEARTBEAT_MAX_AGE_SECONDS - 1)
        stale_response = self.client.get("/health/ready")
        self.assertEqual(stale_response.status_code, 503)

    def test_metrics_endpoint_returns_fixed_length_series(self):
        response = self.client.get("/api/metrics")
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["totals"]["events_total"], 12)
        self.assertEqual(body["totals"]["revenue_cents"], 2599)
        self.assertEqual(len(body["series"]), app_module.SERIES_BUCKETS)
        self.assertTrue(all("timestamp_ms" in point for point in body["series"]))
        self.assertEqual(body["series"][-1]["events"], 2)


if __name__ == "__main__":
    unittest.main()
