# StreamStore — Kafka Order Processing

An event-driven order processing project built with **React, FastAPI, Apache Kafka, and SQLite**.

Started from a tutorial-based producer and consumer, then extended with an order interface and three independent consumer services.

> Payments and notifications are simulated. No money is charged or email sent.

## Features

- React form for placing orders.
- FastAPI backend for validation and publishing events.
- Independent payment, notification, and analytics consumers.
- SQLite storage that persists results across service restarts.
- Dashboard displaying processing results and order statistics.
- Duplicate handling using unique order IDs.

## Architecture

```text
React → FastAPI Order API → Kafka: orders
                                ├── Payment → SQLite
                                ├── Notification → SQLite
                                └── Analytics → SQLite
```

Each service uses a **different consumer group**, so every service receives every order.

Consumers save results before committing their Kafka offsets. If an event is replayed, its unique order ID prevents duplicate database records.

The frontend polls the consumer APIs every two seconds to display results.

## Tech Stack

| Component | Technology |
| --- | --- |
| Frontend | React, Vite |
| Backend | Python, FastAPI |
| Event streaming | Apache Kafka |
| Storage | SQLite |
| Local infrastructure | Docker Compose |

