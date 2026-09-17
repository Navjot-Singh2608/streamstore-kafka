"""Run explicitly against local Kafka: python -m tests.smoke (or python tests/smoke.py)."""
import sys
import time
from contextlib import ExitStack
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from backend.orders import app as orders
from backend.payment import app as payment
from backend.notification import app as notification
from backend.analytics import app as analytics

with ExitStack() as stack:
    producer = stack.enter_context(TestClient(orders))
    clients = [stack.enter_context(TestClient(app)) for app in [payment, notification, analytics]]
    assert producer.post('/orders', json={'user': ' ', 'item': 'coffee', 'quantity': 1}).status_code == 422
    assert producer.post('/orders', json={'user': 'Test', 'item': 'unknown', 'quantity': 1}).status_code == 422
    response = producer.post('/orders', json={'user': 'Kafka smoke test', 'item': 'coffee', 'quantity': 2})
    assert response.status_code == 202, response.text
    event = response.json()['order']
    assert event['total_cents'] == 700
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        found = [[row for row in client.get('/records').json() if row['order_id'] == event['order_id']] for client in clients]
        if all(found):
            break
        time.sleep(.25)
    assert all(found), 'Not all consumers received the order'
    assert [rows[0]['status'] for rows in found] == ['simulated_paid', 'simulated_sent', 'counted']
    print('PASS: validated API -> real Docker Kafka -> all three durable consumers', flush=True)
    print(response.json()['kafka'], flush=True)
print('PASS: graceful shutdown of producer and consumers', flush=True)
