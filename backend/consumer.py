"""One background Kafka loop per FastAPI service. Run with one Uvicorn worker."""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Thread

from confluent_kafka import Consumer, KafkaException
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from backend.models import BOOTSTRAP_SERVERS, TOPIC, OrderEvent
from backend.store import Store

log = logging.getLogger(__name__)


def create_consumer_app(role):
    state = {"running": False, "last_error": None, "assigned_partitions": 0}

    @asynccontextmanager
    async def lifespan(app):
        store = Store(Path(os.getenv("DATA_DIR", "data")) / f"{role}.db")
        app.state.store = store
        stop = Event()

        def consume():
            consumer = None
            try:
                consumer = Consumer({
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "group.id": f"streamstore-{role}-v1",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                })
                consumer.subscribe([TOPIC])
                state["running"] = True
                while not stop.is_set():
                    message = consumer.poll(1)
                    state["assigned_partitions"] = len(consumer.assignment())
                    if message is None:
                        continue
                    if message.error():
                        raise KafkaException(message.error())
                    try:
                        order = OrderEvent.model_validate_json(
                            message.value() or b"")
                    except ValidationError as exc:
                        # Preserve invalid events locally before advancing past them.
                        position = f"{message.topic()}:{message.partition()}:{message.offset()}"
                        store.reject(position, str(exc))
                        log.warning("Rejected invalid order at %s", position)
                    else:
                        store.process(role, order)
                    # Save first, commit second. A crash between them replays safely.
                    consumer.commit(message=message, asynchronous=False)
            except Exception as exc:
                state["last_error"] = str(exc)
                log.exception(
                    "Consumer stopped; restart this service after fixing the error")
            finally:
                state["running"] = False
                if consumer is not None:
                    consumer.close()

        thread = Thread(target=consume, name=f"{role}-consumer", daemon=True)
        thread.start()
        yield
        stop.set()
        await asyncio.to_thread(thread.join)

    app = FastAPI(title=f"StreamStore {role.title()}", lifespan=lifespan)

    @app.get("/health")
    def health():
        healthy = state["running"] and state["last_error"] is None
        return JSONResponse({"service": role, **state}, status_code=200 if healthy else 503)

    @app.get("/records")
    def records():
        return app.state.store.records()

    @app.get("/summary")
    def summary():
        return app.state.store.summary()

    return app
