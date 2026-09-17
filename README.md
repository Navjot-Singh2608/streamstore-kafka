# StreamStore: a beginner Kafka order lab

The original producer.py and tracker.py remain usable. The new React page talks to a FastAPI order producer. Three independent FastAPI services consume the same `orders` topic: payment, notification, and analytics.

Payments and notifications are simulations. No money is charged and no email is sent. Notification means "order received", not "payment succeeded". Analytics measures ordered value, not actual revenue. Prices use integer cents in CAD.

## Run locally (PowerShell)

Keep your existing Kafka Docker container running. If it is stopped, run `docker compose up -d` from this directory. This project runs Python and React on your computer, so Kafka's advertised `localhost:9092` listener is correct. Do not move the APIs into containers without adding an internal Kafka listener.

Install dependencies from the project root:

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
cd frontend
npm install
cd ..
```

Open five terminals in the project directory. Run one command in each:

```powershell
.\venv\Scripts\python.exe -m uvicorn backend.orders:app --port 8000
.\venv\Scripts\python.exe -m uvicorn backend.payment:app --port 8001
.\venv\Scripts\python.exe -m uvicorn backend.notification:app --port 8002
.\venv\Scripts\python.exe -m uvicorn backend.analytics:app --port 8003
npm --prefix frontend run dev
```

Open http://localhost:5173. Place an order and allow a few seconds for initial consumer group assignment. Keep these terminals running. Ctrl+C stops each process. If you do not have a venv, create one with `python -m venv venv` first. Use Node 22.12+ for the frontend.

The Vite development proxy forwards `/api/orders` to port 8000 and the other service prefixes to ports 8001–8003. The browser does not connect directly to Kafka. `npm run build` produces frontend assets; a deployed build would need an HTTP reverse proxy implementing the same routes.

Explore the FastAPI Swagger pages at http://localhost:8000/docs through http://localhost:8003/docs. Consumers expose `/records`, `/summary`, and `/health`. Health describes the local consumer loop and assignment, not a full broker connectivity check. If a consumer stops with an error, fix the cause and restart its terminal.

## Follow one order

```text
React form --HTTP--> Order API --produce--> Kafka: orders
                                              |
                 +----------------------------+-----------------------+
                 v                            v                       v
          payment group                notification group       analytics group
          payment.db                   notification.db          analytics.db
```

1. The API validates input, calculates the price, creates an order ID, and publishes JSON keyed by that ID.
2. It waits for Kafka's delivery acknowledgement before returning HTTP 202. That means accepted, not fully processed.
3. Each consumer reads the event, records its own result in SQLite, then commits its offset.
4. React polls HTTP endpoints every two seconds to show the separate results. There is no guaranteed ordering between the three services.

## The Kafka concepts to learn now

- **Topic:** `orders` is the named event stream. Reading an event does not delete it.
- **Producer:** both the original script and the new order API publish events.
- **Consumer group:** `streamstore-payment-v1`, `streamstore-notification-v1`, and `streamstore-analytics-v1` each receive all orders independently. Sharing one group across different business services would distribute orders between them instead.
- **Partition and key:** order ID is the key. Kafka preserves order within a partition; it does not guarantee global order across partitions. The page shows the partition and offset from the producer acknowledgement.
- **Offset:** a group's saved reading position. `earliest` applies when the group has no valid committed offset; it does not rewind every restart.
- **At-least-once delivery:** processing can succeed before an offset commit fails. Kafka may replay that event. A unique order ID in SQLite makes this demo's database effect idempotent.
- **Background work:** FastAPI lifespan starts a polling thread and stops it on shutdown. HTTP routes stay responsive while Kafka is polled.

The three tiny service entry points use `backend/consumer.py` for the same poll/validate/save/commit loop. Business results are in `backend/store.py`. Read `backend/orders.py` next to the original producer to compare script-based and HTTP-based publishing.

## Try these experiments

1. Place two orders. Check that all three consumers see both and analytics counts the quantities.
2. Stop only notification with Ctrl+C. Place another order. Payment and analytics continue. Restart notification: it catches up from its committed offset.
3. Run `python producer.py` using the venv. Old-style events still work; they have no price so contribute zero ordered value.
4. Read `tracker.py`: its `order-tracker` group receives the same orders without taking them away from the other groups.
5. Inspect the groups:

```powershell
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

Compare current offset, log end offset, and lag while notification is stopped, then restarted.

## Scope and limitations

Use one Uvicorn worker per consumer service. SQLite results persist in `data/`; do not delete these files while keeping committed Kafka offsets and expect old results to reappear. Old events replay only while Kafka retains them and after a deliberate group reset. This demo is local development code without authentication or deployment hardening.

Invalid events are saved in a local `rejected` SQLite table and skipped after that save; processing/storage/commit failures stop the consumer instead of silently skipping an order. Real payment/email calls would need their own idempotency mechanisms; a local unique database row is not enough for external side effects. An HTTP retry creates a new order ID, so a delivery timeout has an ambiguous outcome and manual retries can duplicate orders.

Your existing Docker Compose mounts `/var/lib/kafka/data`, but configures logs in `/tmp/kraft-combined-logs`. Therefore it does not currently persist Kafka logs in that named volume. It is left unchanged to avoid disrupting your running learning broker. Before relying on container recreation for persistence, back up needed events and deliberately align the log path with the mounted volume.

Next steps after these concepts feel comfortable: payment-result events followed by notifications, multiple partitions with consumers in the same group, retries/dead-letter topics, and HTTP idempotency. No need for real payment gateways or a larger microservice stack yet.

## Checks

```powershell
.\venv\Scripts\python.exe -m unittest discover -s tests -v
npm --prefix frontend run build
```

References: [Confluent Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html), [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/).
