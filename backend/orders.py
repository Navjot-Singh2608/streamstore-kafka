"""HTTP producer: a successful response means Kafka acknowledged the order."""
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from threading import Event

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from backend.models import BOOTSTRAP_SERVERS, TOPIC, CATALOG, OrderRequest


@asynccontextmanager
async def lifespan(app):
    app.state.producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "message.timeout.ms": 5000,
    })
    yield
    app.state.producer.flush(6)


app = FastAPI(title="StreamStore Orders", lifespan=lifespan)


@app.get("/products")
def products():
    return [{"name": name, "price_cents": price} for name, price in CATALOG.items()]


@app.post("/orders", status_code=202)
def create_order(order: OrderRequest):
    if order.item not in CATALOG:
        raise HTTPException(422, "Choose an item from the product catalog")
    event = {
        **order.model_dump(), "user": order.user.strip(),
        "order_id": str(uuid.uuid4()),
        "total_cents": CATALOG[order.item] * order.quantity,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    done, errors, metadata = Event(), [], {}

    def delivered(error, message):
        if error:
            errors.append(str(error))
        else:
            metadata.update(topic=message.topic(), partition=message.partition(), offset=message.offset())
        done.set()

    try:
        app.state.producer.produce(TOPIC, key=event["order_id"],
                                   value=json.dumps(event).encode(), on_delivery=delivered)
        # Sync routes run in FastAPI's thread pool; polling does not block its event loop.
        while not done.is_set():
            app.state.producer.poll(0.1)
    except Exception as exc:
        raise HTTPException(503, "Kafka unavailable; order could not be confirmed") from exc
    if errors:
        raise HTTPException(503, "Kafka did not confirm the order. Check Kafka before retrying.")
    return {"order": event, "kafka": metadata, "status": "accepted"}
