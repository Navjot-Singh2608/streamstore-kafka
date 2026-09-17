"""SQLite is a small durable read model; Kafka remains the event stream."""
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS results (order_id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS rejected (position TEXT PRIMARY KEY, reason TEXT NOT NULL)")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            with db:
                yield db
        finally:
            db.close()

    def process(self, role, order):
        record = order.model_dump()
        record["processed_at"] = datetime.now(timezone.utc).isoformat()
        if role == "payment":
            record.update(status="simulated_paid", message="Demo payment recorded; no money charged")
        elif role == "notification":
            record.update(status="simulated_sent", message=f"Thanks {order.user}! Your order was received. No email was sent.")
        else:
            record.update(status="counted", message="Included in order analytics")
        # A replay of the same order_id cannot charge/count/record it twice.
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO results VALUES (?, ?)", (order.order_id, json.dumps(record)))

    def reject(self, position, reason):
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO rejected VALUES (?, ?)", (position, reason))

    def records(self, limit=50):
        with self.connect() as db:
            rows = db.execute("SELECT payload FROM results ORDER BY rowid DESC LIMIT ?", (limit,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def summary(self):
        with self.connect() as db:
            row = db.execute("SELECT COUNT(*), COALESCE(SUM(json_extract(payload, '$.quantity')), 0), COALESCE(SUM(json_extract(payload, '$.total_cents')), 0) FROM results").fetchone()
        return dict(orders=row[0], items=row[1], ordered_value_cents=row[2])

