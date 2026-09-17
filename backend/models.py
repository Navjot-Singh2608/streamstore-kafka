import os
from pydantic import BaseModel, Field

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "orders")
CATALOG = {"frozen yogurt": 600, "coffee": 350, "sandwich": 850}


class OrderRequest(BaseModel):
    user: str = Field(min_length=1, max_length=80, pattern=r"\S")
    item: str = Field(min_length=1, max_length=80)
    quantity: int = Field(ge=1, le=100, strict=True)


class OrderEvent(OrderRequest):
    order_id: str = Field(min_length=1, max_length=100)
    # Defaults let the original producer's events remain valid.
    total_cents: int = Field(default=0, ge=0, strict=True)
    created_at: str = ""
