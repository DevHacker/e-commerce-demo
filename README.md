# Reactive E-commerce Microservices — Saga, Outbox & CDC

![Java](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.1%20WebFlux-6DB33F)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1%20(KRaft)-231F20)
![Debezium](https://img.shields.io/badge/Debezium-3.7-91C73E)

A fully **reactive** e-commerce backend built as a set of independent Spring Boot microservices that coordinate
an order through a **saga orchestrator**, with **reliable messaging** guaranteed by the **Transactional Outbox**
pattern, **Debezium change data capture** and **idempotent (inbox) consumers**.

No distributed transactions, no dual writes, no lost or duplicated side effects — every state change and the
message announcing it are committed in the same local database transaction.

---

## Table of contents

- [Highlights](#highlights)
- [Architecture](#architecture)
- [Services](#services)
- [The order saga](#the-order-saga)
- [Reliable messaging](#reliable-messaging)
- [Tech stack](#tech-stack)
- [Getting started](#getting-started)
- [API reference](#api-reference)
- [Payment methods](#payment-methods)
- [Testing](#testing)
- [Project structure](#project-structure)
- [Design documents](#design-documents)
- [Roadmap](#roadmap)

---

## Highlights

- **100 % non-blocking**: Spring WebFlux functional routes + R2DBC (`DatabaseClient`, no JPA).
- **Saga orchestration** in the Order service, driven by `SagaMachine`, a set of **pure functions**
  `(state, event) → decision` — no state-machine framework, no I/O, trivially unit-testable.
- **Transactional Outbox + Debezium CDC**: services never call Kafka directly; they insert into `outbox_events`
  and Debezium's Outbox Event Router publishes to the right topic.
- **Inbox pattern**: every consumer records processed event ids in the same transaction as its effects —
  exactly-once *effects* on top of at-least-once delivery.
- **Compensations** for payment failure, customer cancellation and **saga timeouts** (background sweeper).
- **High-concurrency stock reservation**: stock split into **buckets** reserved with single atomic conditional
  `UPDATE`s — no long-held locks, no retry storms, no phantom reservations.
- **Strategy pattern** for payments: credit card, cash on delivery, PayPal, Razorpay, Stripe.
- **Dead-letter topics**, manual acks, `read_committed` consumers, retention purging of inbox/outbox tables.
- **Idempotent API**: `POST /orders` requires an `Idempotency-Key` header.
- Unit, integration (Testcontainers) and **end-to-end tests** running the full stack through Debezium.

## Architecture

```
                         ┌──────────────────────────────────────────────┐
     HTTP clients ──────►│ order-service  (saga orchestrator)  :8080    │
                         │   orders · saga_state · outbox · inbox       │
                         └───────┬──────────────────────────▲───────────┘
                                 │ outbox_events            │ order.saga.replies
                                 ▼  (Debezium CDC)          │
   ┌──────────────────────────────────────────────────────────────────────────────┐
   │                               Apache Kafka (KRaft)                           │
   │  product.commands · inventory.commands · payment.commands                    │
   │  order.saga.replies · order.events · <topic>.DLT                             │
   └──────┬────────────────────────┬────────────────────────┬─────────────────────┘
          ▼                        ▼                        ▼
 ┌──────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────┐
 │ product-service  │   │ inventory-service    │   │ payment-service      │   │ customer-service │
 │ :8081 product_db │   │ :8082 inventory_db   │   │ :8084 payment_db     │   │ :8085 customer_db│
 └──────────────────┘   └──────────────────────┘   └──────────────────────┘   └──────────────────┘
        Each service owns its PostgreSQL database and publishes through its own outbox + Debezium connector.
```

## Services

| Module | Port | Database (host port) | Responsibility |
|---|---|---|---|
| `order-service` | 8080 | `order_db` (5442) | Orders API and **saga orchestrator**; timeout sweeper; publishes `order.events` |
| `product-service` | 8081 | `product_db` (5441) | Product catalogue; validates and prices order lines for the saga |
| `inventory-service` | 8082 | `inventory_db` (5443) | Bucketed stock; reserves and releases stock for the saga |
| `payment-service` | 8084 | `payment_db` (5444) | Charges and refunds, one **strategy per payment method** |
| `customer-service` | 8085 | `customer_db` (5445) | Customer registry (standalone for now) |
| `common` | — | — | Shared library: outbox writer, inbox, message dispatcher, R2DBC / Flyway / Kafka auto-configuration ([README](common/README.md)) |
| `e2e-tests` | — | — | End-to-end tests over the whole system, through Debezium |

## The order saga

```
POST /orders ─► Order ──ValidateProducts──► Product
                  ▲  ◄──ProductsValidated──┘
                  ├────ReserveStock────────► Inventory
                  ├────ProcessPayment──────► Payment ──► strategy of the order's method
                  └─ on cancel / failure: RefundPayment, then ReleaseStock
```

Saga state transitions (`SagaMachine`):

```
STARTED ──ProductsValidated──► PRODUCTS_VALIDATED ──StockReserved──► STOCK_RESERVED ──PaymentCompleted──► COMPLETED
   │                                  │                                   │                                 │
   └─ProductsRejected─► REJECTED      └─StockRejected─► REJECTED          │                               cancel
                                                                          │                                 │
 PaymentFailed: COMPENSATING(ReleaseStock) ──StockReleased──► COMPENSATED, order REJECTED                   │
 cancel:        COMPENSATING(RefundPayment) ──PaymentRefunded──► COMPENSATING(ReleaseStock) ◄───────────────┘
                ──StockReleased──► COMPENSATED, order CANCELLED
 timeout:       STARTED → REJECTED · PRODUCTS_VALIDATED → release stock · STOCK_RESERVED → refund, then release
```

Order status as seen by clients: `PENDING → CONFIRMED | REJECTED`, and `CONFIRMED → CANCELLING → CANCELLED`.
Every step is recorded in a saga step history, available at `GET /orders/{id}/history`.

## Reliable messaging

1. A handler changes its business tables **and** inserts a row into `outbox_events` in **one** R2DBC transaction.
2. A Debezium connector per database tails the WAL and routes each outbox row to its topic
   (Outbox Event Router SMT, key = aggregate id, event id and type in headers).
3. The consumer's `MessageDispatcher` runs the handler and the `processed_events` (inbox) insert in **one**
   transaction; a redelivered message is detected and skipped.
4. Failures are retried; poison messages land in `<topic>.DLT`.
5. `RetentionPurger` periodically purges old inbox and outbox rows.

## Tech stack

| Concern | Choice |
|---|---|
| Language / runtime | Java 21 |
| Framework | Spring Boot 4.1.1, Spring WebFlux (functional routing) |
| Persistence | R2DBC PostgreSQL + r2dbc-pool, PostgreSQL 18 (one database per service) |
| Migrations | Flyway (run at startup) |
| Messaging | Apache Kafka 4.1 (KRaft), Spring Kafka consumers |
| CDC | Debezium Connect 3.7 (Outbox Event Router) |
| Testing | JUnit 5, Reactor Test, Testcontainers |
| Tooling | Maven, Docker Compose, Kafka UI (optional) |

## Getting started

### Prerequisites

- JDK 21
- Maven 3.9+
- Docker with Docker Compose v2

### 1. Build

```bash
mvn install          # builds every module and runs unit tests (no Docker needed)
```

### 2. Start the infrastructure

```bash
cd infra
docker compose up -d --wait        # 5 × PostgreSQL, Kafka, Debezium Connect
docker compose up kafka-init       # creates the topics and their .DLT topics
# optional Kafka UI on http://localhost:8089
docker compose --profile ui up -d
```

| Component | Host address |
|---|---|
| Kafka | `localhost:29092` |
| Debezium Connect | `http://localhost:8083` |
| Kafka UI | `http://localhost:8089` |

### 3. Start the services

```bash
java -jar product-service/target/product-service-1.0.0-SNAPSHOT.jar
java -jar inventory-service/target/inventory-service-1.0.0-SNAPSHOT.jar
java -jar payment-service/target/payment-service-1.0.0-SNAPSHOT.jar
java -jar customer-service/target/customer-service-1.0.0-SNAPSHOT.jar
java -jar order-service/target/order-service-1.0.0-SNAPSHOT.jar
```

Database and Kafka settings can be overridden with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
and `KAFKA_BOOTSTRAP_SERVERS`.

### 4. Register the Debezium connectors

Once the services have created their outbox tables (Flyway runs at startup):

```bash
cd infra && ./scripts/register-connectors.sh
```

### 5. Place an order

```bash
# create a product and give it stock
curl -s -X POST localhost:8081/products -H 'Content-Type: application/json' \
  -d '{"sku": "TSHIRT-001", "name": "T-shirt", "price": 19.99}'
curl -s -X PUT  localhost:8082/stock/<productId> -H 'Content-Type: application/json' \
  -d '{"quantity": 100, "bucketCount": 4}'

# order it
curl -i -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{
        "customerRef": "c1",
        "items": [{"productId": "<uuid>", "quantity": 2}],
        "payment": {"method": "stripe", "details": {"paymentMethodId": "pm_card_visa"}}
      }'

curl localhost:8080/orders/<orderId>            # PENDING -> CONFIRMED | REJECTED
curl localhost:8080/orders/<orderId>/history    # saga steps
curl -X POST localhost:8080/orders/<orderId>/cancel
```

## API reference

| Service | Method & path | Description |
|---|---|---|
| Order | `POST /orders` | Place an order (requires `Idempotency-Key`) |
| Order | `GET /orders/{id}` | Order and its status |
| Order | `GET /orders/{id}/history` | Saga step history |
| Order | `POST /orders/{id}/cancel` | Cancel a confirmed order (refund, then release stock) |
| Product | `POST /products`, `GET /products`, `GET /products/{id}`, `PUT /products/{id}` | Catalogue management (optimistic versioning) |
| Inventory | `GET /stock/{productId}`, `PUT /stock/{productId}` | Read / set a product's stock |
| Payment | `GET /payments/methods` | Supported methods and the details each one needs |
| Payment | `GET /payments/{orderId}` | Payment of an order |
| Customer | `POST /customers`, `GET /customers`, `GET /customers/{id}`, `PUT /customers/{id}` | Customer registry |

Every service also exposes Spring Boot Actuator `health`, `info` and `metrics`.

## Payment methods

Method names are case-insensitive. Gateways are **simulated** (deterministic, no credentials); the design
addendum explains how to plug in a real SDK.

| Method | Required details | Test values that decline |
|---|---|---|
| Credit card | `cardToken` | `tok_declined`, `tok_insufficient_funds` |
| Cash on delivery | `deliveryAddress` | — |
| PayPal | `payerEmail` | any address starting with `declined@` |
| Stripe | `paymentMethodId` (`pm_…`) | `pm_card_chargeDeclined`, `pm_card_chargeDeclinedInsufficientFunds` |
| Razorpay | `razorpayPaymentId` (`pay_…`), `razorpaySignature` | — |

## Testing

| Command | Scope |
|---|---|
| `mvn test` | Unit tests of every module (pure domain logic, validation, dispatcher) — no Docker |
| `mvn verify` | + integration tests (`*IT`) with Testcontainers — Docker required |
| `mvn -DskipTests package && mvn -Pe2e verify` | End-to-end tests over the full stack through Debezium (stop any local dev stack first) |

End-to-end scenarios include the happy path, rejections, cancellation, saga timeouts, message replay and
concurrent stock reservation.

## Project structure

```
ecommerce/
├── common/               shared library (outbox, inbox, dispatcher, auto-configuration)
├── order-service/        orders API + saga orchestrator
├── product-service/      catalogue
├── inventory-service/    bucketed stock
├── payment-service/      payment strategies
├── customer-service/     customer registry
├── e2e-tests/            full-stack tests (Maven profile e2e)
├── infra/
│   ├── docker-compose.yml
│   ├── debezium/         one outbox connector per service
│   └── scripts/          connector registration, smoke test
└── docs/superpowers/     design specs and implementation plans
```

## Design documents

- [`docs/superpowers/specs/`](docs/superpowers/specs/) — phase 1 design (saga, outbox, Kafka topology, data model,
  concurrency) and the payment / customer addendum.
- [`docs/superpowers/plans/`](docs/superpowers/plans/) — step-by-step implementation plans.

## Roadmap

- Authentication and authorisation
- Caching
- Integrate `customer-service` into the saga
- Real payment gateway SDKs
- Containerised services and deployment manifests
