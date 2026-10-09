import unittest

from analytics_processor import InvalidEvent, parse_event


class ParseEventTests(unittest.TestCase):
    def test_parses_purchase_and_rounds_minor_units(self):
        event = parse_event(
            b'{"event_id":"evt-1","user_id":"shopper-1","event_type":"purchase",'
            b'"amount":"12.345","currency":"USD"}'
        )
        self.assertEqual(event.event_id, "evt-1")
        self.assertEqual(event.user_id, "shopper-1")
        self.assertEqual(event.event_type, "purchase")
        self.assertEqual(event.revenue_cents, 1235)

    def test_non_purchase_amount_does_not_count_as_revenue(self):
        event = parse_event(
            {
                "event_id": "evt-2",
                "user_id": "shopper-2",
                "event_type": "add_to_cart",
                "amount": "49.99",
                "currency": "USD",
            }
        )
        self.assertEqual(event.revenue_cents, 0)

    def test_rejects_non_object_json(self):
        with self.assertRaises(InvalidEvent):
            parse_event(b'["not", "an", "event"]')

    def test_rejects_invalid_utf8_and_json(self):
        with self.assertRaises(InvalidEvent):
            parse_event(b"\xff\xfe")
        with self.assertRaises(InvalidEvent):
            parse_event(b"{broken json")

    def test_rejects_oversized_event(self):
        with self.assertRaises(InvalidEvent):
            parse_event(b" " * 101, max_bytes=100)

    def test_rejects_unknown_event_type_and_currency(self):
        base = {"event_id": "evt-3", "user_id": "shopper-3", "event_type": "refund"}
        with self.assertRaises(InvalidEvent):
            parse_event(base)

        base["event_type"] = "purchase"
        base["currency"] = "EUR"
        with self.assertRaises(InvalidEvent):
            parse_event(base)

    def test_rejects_non_finite_or_negative_amount(self):
        for amount in ("NaN", "Infinity", "-0.01"):
            with self.subTest(amount=amount), self.assertRaises(InvalidEvent):
                parse_event(
                    {
                        "event_id": "evt-4",
                        "user_id": "shopper-4",
                        "event_type": "purchase",
                        "amount": amount,
                        "currency": "USD",
                    }
                )

    def test_high_precision_amount_is_safely_rounded(self):
        event = parse_event(
            {
                "event_id": "evt-precise",
                "user_id": "shopper-precise",
                "event_type": "purchase",
                "amount": "0." + "1" * 100,
                "currency": "USD",
            }
        )
        self.assertEqual(event.revenue_cents, 11)

    def test_rejects_surrogate_unicode_in_identifiers(self):
        with self.assertRaises(InvalidEvent):
            parse_event(b'{"event_id":"\\ud800","user_id":"shopper","event_type":"page_view"}')

    def test_rejects_control_characters_and_missing_identifiers(self):
        for payload in (
            {"event_id": "evt-5", "user_id": "", "event_type": "page_view"},
            {"event_id": "evt-6\n", "user_id": "shopper-6", "event_type": "page_view"},
        ):
            with self.subTest(payload=payload), self.assertRaises(InvalidEvent):
                parse_event(payload)


if __name__ == "__main__":
    unittest.main()
