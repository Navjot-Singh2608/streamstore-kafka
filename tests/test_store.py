import tempfile
import unittest
from pathlib import Path
from backend.models import OrderEvent
from backend.store import Store

class StoreTests(unittest.TestCase):
    def test_replay_and_restart_do_not_double_count(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "analytics.db"
            order = OrderEvent(order_id="same-order", user="Lara", item="coffee", quantity=2, total_cents=700)
            Store(path).process("analytics", order)
            restarted = Store(path)
            restarted.process("analytics", order)
            self.assertEqual(restarted.summary(), {"orders": 1, "items": 2, "ordered_value_cents": 700})

    def test_legacy_producer_event(self):
        order = OrderEvent.model_validate({"order_id": "old", "user": "lara", "item": "frozen yogurt", "quantity": 11})
        self.assertEqual(order.total_cents, 0)

if __name__ == "__main__":
    unittest.main()
