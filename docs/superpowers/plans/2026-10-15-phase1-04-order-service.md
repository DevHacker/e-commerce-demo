# Phase 1, plan 4: Order service (saga orchestrator)

> **Update:** the saga now includes a payment step (`STOCK_RESERVED` is stored, `ProcessPayment`/`RefundPayment` added, refund before stock release, `payment` required on `POST /orders`). The code in this plan is the version before that change; the current code is in `order-service/` and the change is described in `docs/superpowers/specs/2026-10-15-phase2-payment-customer-design.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `order-service`: the orders API (idempotent creation, read, history, cancellation) and the saga orchestrator that drives Product validation then Inventory reservation, compensates on cancellation, and rejects or compensates sagas that stop making progress.

**Architecture:** Same shape as plans 2 and 3, on `shop-common`. The orchestrator is split in two: `SagaMachine` is a set of pure functions `(order, saga, event) -> Decision` with no I/O; `SagaCoordinator` is the thin imperative shell that loads state, asks the machine, and applies the resulting `Transition` (saga row, order row, histories, outbox commands and events) inside one transaction. Order creation writes order, items, saga, both histories and the first command in a single transaction, so there is no window between "order exists" and "saga started".

**Tech Stack:** Java 21, Spring Boot 4.1.1 (WebFlux), R2DBC `DatabaseClient`, Flyway, PostgreSQL 18, Kafka via Spring Kafka (inside `shop-common`), Testcontainers 2.0.5.

**Spec:** `docs/superpowers/specs/2026-10-15-ecommerce-phase1-design.md` (sections 4, 5, 6, 7, 9, 10)

**Prerequisite:** plans 1 to 3 done. This service does not call the others; it only needs `shop-common` in `~/.m2` and, for the manual check, the infrastructure of plan 1. Work on a branch, for example `feat/order-service`.

## Verification status (read before starting)

The code in this plan was compiled and tested before being written here, in a scratch copy:

- 20 unit tests (`SagaMachineTest` 13, `PricingTest` 2, `RequestValidationTest` 5): green.
- 14 integration tests (`OrderApiIT` 6, `SagaFlowIT` 6, `SagaTimeoutIT` 2): green, five runs in a row, against **PostgreSQL 16** and an **embedded Kafka (KRaft) broker**, because the authoring sandbox had no Docker daemon. They play Product and Inventory by publishing their replies, and read the commands and events the saga wrote to its outbox.

**Not executed yet, so Tasks 5 to 8 (Testcontainers) and Task 9 (Debezium) are the first real run of:** the Testcontainers support classes, PostgreSQL 18, and the loop through Debezium. The end-to-end test with the three services together is plan 5.

## Decisions

1. **`STOCK_RESERVED` is not stored.** The spec lists `STARTED -> PRODUCTS_VALIDATED -> STOCK_RESERVED -> COMPLETED`. The reply that reserves the stock completes the saga in the same transition, so `STOCK_RESERVED` would never be observable. The `saga_state` CHECK constraint lists the six states that exist; the step history still records `ReserveStock: RESERVED`.
2. **Saga id = order id**, which is also the Kafka key. Per-order ordering is guaranteed by the topic partitioning; replies for one order are processed one at a time.
3. **Optimistic locking on `orders` and `saga_state`** (`WHERE version = :expected`), as the spec decided. A lost race raises `ConcurrentUpdateException`: in a Kafka handler the transaction rolls back and the consumer replays the message on fresh state; in `POST /orders/{id}/cancel` it is retried 3 times, then answered `409`.
4. **Duplicates and late replies are ignored, not errors.** `SagaMachine` returns `Ignore` for a reply that does not fit the saga's state (a duplicate, or an answer arriving after a timeout). The inbox already drops redelivery of the same event id; `Ignore` covers a different event id carrying the same news. A reply for an order that does not exist goes to `order.saga.replies.DLT`.
5. **Timeout policy** (`shop.order.saga-timeout`, default 2 minutes, checked every `sweep-interval`): `STARTED` becomes `REJECTED` (nothing reserved yet); `PRODUCTS_VALIDATED` becomes `COMPENSATING` with a `ReleaseStock` (a reservation may exist; `ReserveStock` and `ReleaseStock` travel on the same topic and key, so the release is always processed after the reserve); `COMPENSATING` sends `ReleaseStock` again (Inventory's release is idempotent). The sweeper re-checks staleness on the freshly loaded saga, handles each saga in its own transaction, and skips a saga that moved meanwhile, so it is safe on several instances.
6. **A reactive `@Scheduled` method is called once and its `Mono` is re-subscribed on every run.** Anything time-dependent must be computed at subscription (`Mono.defer`), otherwise the cutoff stays frozen at startup. This bug was found by `SagaTimeoutIT` and also existed in `shop-common`'s purge job; plan 1 now contains the fix and a regression test (`RetentionJobTest`), and `shop-common` was reinstalled.
7. **Test hygiene.** `SagaFlowIT` and `SagaTimeoutIT` use `@DirtiesContext(AFTER_CLASS)`, so a finished class does not leave a consumer and a sweeper running against the database of the next class (one run in seven hung on a first request before this). `SagaTimeoutIT` first waits for its Kafka listener to be assigned (a reply for an unknown order must reach the DLT), because a first consumer-group join can take longer than the 2 second timeout under test.
8. **Flyway versions:** `shop-common` owns `V1`; this service starts at `V2`.

## File structure

```
order-service/
  pom.xml
  src/main/resources/application.yml
  src/main/resources/db/migration/V2__orders.sql
  src/main/java/com/shop/order/
    OrderServiceApplication
    domain/      OrderStatus, SagaState, Records, SagaMessages, Pricing,
                 SagaMachine                                   (pure)
                 OrderExceptions
    repository/  OrderRepository                               <- all SQL
    service/     SagaOutbox, SagaCoordinator, OrderService,
                 OrderProperties, SagaTimeoutSweeper
    web/         OrderDtos, RequestValidation (pure), InvalidRequestException,
                 OrderHandler, OrderRoutes                     <- functional endpoints
    messaging/   ReplyMessages, SagaReplyHandlers,
                 OrderMessagingConfig, SagaRepliesListener
  src/test/java/com/shop/order/
    domain/{PricingTest,SagaMachineTest}, web/RequestValidationTest      (unit)
    OrderApiIT, SagaFlowIT, SagaTimeoutIT                               (Docker)
    support/{PostgresSupport,KafkaSupport,KafkaTestClient,SagaTestBase}
```

Conventions: unit tests are `*Test` (Surefire, no Docker); integration tests are `*IT` (Failsafe, Docker needed). Commands run from the `ecommerce/` directory unless stated.

## Contracts

HTTP:

| Request | Result |
|---|---|
| `POST /orders` with header `Idempotency-Key` and `{"customerRef":"c1","items":[{"productId":"<uuid>","quantity":2}]}` | `202` `{"orderId","status":"PENDING"}` + `Location`; the same key returns the same order; `400` with every violation listed |
| `GET /orders/{id}` | `200` order with items; `total` and `unitPrice` are `null` until the products are validated; `404` |
| `GET /orders/{id}/history` | `200` `{"status":[...],"steps":[...]}`: `order_status_history` and `saga_step_history` in order |
| `POST /orders/{id}/cancel` | `202` (`status` becomes `CANCELLING`) for a `CONFIRMED` order; `409` otherwise; `404` |

Statuses: `PENDING`, `CONFIRMED`, `REJECTED`, `CANCELLING`, `CANCELLED`. Saga states: `STARTED`, `PRODUCTS_VALIDATED`, `COMPLETED`, `REJECTED`, `COMPENSATING`, `COMPENSATED`.

Messages (the order id is the saga id and the Kafka key; commands carry `replyTo = order.saga.replies`):

| Direction | Topic | Type | `data` |
|---|---|---|---|
| out | `product.commands` | `ValidateProducts` | `{"items":[{"productId","quantity"}]}` |
| in | `order.saga.replies` | `ProductsValidated` | `{"items":[{"productId","unitPrice"}]}` |
| in | `order.saga.replies` | `ProductsRejected` | `{"rejections":[{"productId","reason"}]}` |
| out | `inventory.commands` | `ReserveStock` | `{"items":[{"productId","quantity"}]}` |
| in | `order.saga.replies` | `StockReserved` | any (`reservations`) |
| in | `order.saga.replies` | `StockRejected` | `{"shortages":[{"productId","requested","missing"}]}` |
| out | `inventory.commands` | `ReleaseStock` | `{"reason":"ORDER_CANCELLED \| TIMEOUT"}` |
| in | `order.saga.replies` | `StockReleased` | any (`released`) |
| out | `order.events` | `OrderCreated`, `OrderConfirmed`, `OrderRejected`, `OrderCancelled` | `{"orderId","customerRef","status","total","reason"}` |

---

### Task 1: Scaffold the service

**Files:**

- Create: `order-service/pom.xml`
- Create: `order-service/src/main/resources/application.yml`
- Create: `order-service/src/main/resources/db/migration/V2__orders.sql`
- Create: `order-service/src/main/java/com/shop/order/OrderServiceApplication.java`

- [ ] **Step 1: Create the POM (same dependencies as the other services)**

Create `order-service/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.1</version>
        <relativePath/>
    </parent>

    <groupId>com.shop</groupId>
    <artifactId>order-service</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>order-service</name>
    <description>Orders and the saga orchestrator (product validation, stock reservation, cancellation).</description>

    <properties>
        <java.version>21</java.version>
        <shop-common.version>1.0.0-SNAPSHOT</shop-common.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.shop</groupId>
            <artifactId>shop-common</artifactId>
            <version>${shop-common.version}</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webflux</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webflux-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
            <!-- Unit tests: *Test (surefire, no Docker). Integration tests: *IT (failsafe, needs Docker). -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <configuration>
                    <includes>
                        <include>**/*IT.java</include>
                    </includes>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create the configuration (port 8080, database `order_db` on host port 5442, saga timeout settings)**

Create `order-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: order-service

server:
  port: 8080

shop:
  r2dbc:
    host: ${DB_HOST:localhost}
    port: ${DB_PORT:5442}
    database: ${DB_NAME:order_db}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:postgres}
    postgres:
      application-name: order-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:29092}
    group-id: order-service
  order:
    # a saga with no progress for this long is compensated or rejected by the sweeper
    saga-timeout: 2m
    sweep-interval: 30s
    sweep-batch-size: 100

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

- [ ] **Step 3: Create the schema: `orders`, `order_items`, `saga_state` (partial index for the sweeper), and the two append-only histories**

Create `order-service/src/main/resources/db/migration/V2__orders.sql`:

```sql
-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.

CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_ref    VARCHAR(100) NOT NULL,
    status          VARCHAR(16)  NOT NULL
        CHECK (status IN ('PENDING', 'CONFIRMED', 'REJECTED', 'CANCELLING', 'CANCELLED')),
    total           NUMERIC(14, 2),            -- set when the products are validated (prices snapshot)
    idempotency_key VARCHAR(100) NOT NULL,
    version         BIGINT       NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key)
);

CREATE TABLE order_items (
    order_id   UUID           NOT NULL REFERENCES orders (id),
    product_id UUID           NOT NULL,
    quantity   INT            NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2),                 -- snapshot taken from the ProductsValidated reply
    PRIMARY KEY (order_id, product_id)
);

-- One row per order; saga_id = order_id. STOCK_RESERVED of the spec's diagram is never stored:
-- the reply that reserves the stock completes the saga in the same transition.
CREATE TABLE saga_state (
    saga_id      UUID PRIMARY KEY REFERENCES orders (id),
    state        VARCHAR(24) NOT NULL
        CHECK (state IN ('STARTED', 'PRODUCTS_VALIDATED', 'COMPLETED', 'REJECTED', 'COMPENSATING', 'COMPENSATED')),
    current_step VARCHAR(32) NOT NULL,
    version      BIGINT      NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- the timeout sweeper looks for stale non-terminal sagas
CREATE INDEX idx_saga_state_pending ON saga_state (updated_at)
    WHERE state IN ('STARTED', 'PRODUCTS_VALIDATED', 'COMPENSATING');

-- Append-only histories, written in the same transaction as the transition they describe.
CREATE TABLE order_status_history (
    id                 BIGSERIAL PRIMARY KEY,
    order_id           UUID        NOT NULL REFERENCES orders (id),
    from_status        VARCHAR(16),
    to_status          VARCHAR(16) NOT NULL,
    reason             VARCHAR(200),
    trigger_message_id UUID,
    occurred_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_status_history_order ON order_status_history (order_id, id);

CREATE TABLE saga_step_history (
    id          BIGSERIAL PRIMARY KEY,
    saga_id     UUID        NOT NULL REFERENCES saga_state (saga_id),
    step        VARCHAR(32) NOT NULL,
    direction   VARCHAR(16) NOT NULL CHECK (direction IN ('FORWARD', 'COMPENSATE')),
    outcome     VARCHAR(32) NOT NULL,
    message_id  UUID,
    detail      JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_step_history_saga ON saga_step_history (saga_id, id);
```

- [ ] **Step 4: Create the application class**

Create `order-service/src/main/java/com/shop/order/OrderServiceApplication.java`:

```java
package com.shop.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
```

- [ ] **Step 5: Compile**

```bash
cd order-service && mvn -B -q compile
```

Expected: no output, exit code 0.

- [ ] **Step 6: Commit**

```bash
git add order-service
git commit -m "build(order): scaffold order-service"
```

### Task 2: Domain types and pricing

**Files:**

- Create: `order-service/src/test/java/com/shop/order/domain/PricingTest.java`
- Create: `order-service/src/main/java/com/shop/order/domain/{OrderStatus,SagaState,Records,SagaMessages,Pricing}.java`

- [ ] **Step 1: Write the failing test**

Create `order-service/src/test/java/com/shop/order/domain/PricingTest.java`:

```java
package com.shop.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.domain.Records.OrderItem;
import org.junit.jupiter.api.Test;

class PricingTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    @Test
    void totalIsTheSumOfQuantityTimesUnitPrice() {
        var items = List.of(new OrderItem(A, 2, null), new OrderItem(B, 3, null));

        var pricing = Pricing.of(items, Map.of(A, new BigDecimal("10.50"), B, new BigDecimal("1.20"))).orElseThrow();

        assertThat(pricing.total()).isEqualByComparingTo("24.60");
        assertThat(pricing.unitPrices()).containsEntry(A, new BigDecimal("10.50"));
    }

    @Test
    void isEmptyWhenAnItemHasNoPrice() {
        var items = List.of(new OrderItem(A, 1, null), new OrderItem(B, 1, null));

        assertThat(Pricing.of(items, Map.of(A, BigDecimal.ONE))).isEmpty();
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd order-service && mvn -B -q test -Dtest=PricingTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... Pricing`.

- [ ] **Step 3: Implement the domain vocabulary: statuses, saga states, persistent records, the replies the saga receives, the commands and events it sends (all plain data), and the pure price snapshot**

Create `order-service/src/main/java/com/shop/order/domain/OrderStatus.java`:

```java
package com.shop.order.domain;

public enum OrderStatus { PENDING, CONFIRMED, REJECTED, CANCELLING, CANCELLED }
```

Create `order-service/src/main/java/com/shop/order/domain/SagaState.java`:

```java
package com.shop.order.domain;

public enum SagaState {
    STARTED, PRODUCTS_VALIDATED, COMPLETED, REJECTED, COMPENSATING, COMPENSATED;

    /** A terminal saga never changes again (a completed order can still be cancelled: that starts COMPENSATING). */
    public boolean terminal() {
        return this == REJECTED || this == COMPENSATED;
    }

    /** States the timeout sweeper watches. */
    public boolean pending() {
        return this == STARTED || this == PRODUCTS_VALIDATED || this == COMPENSATING;
    }
}
```

Create `order-service/src/main/java/com/shop/order/domain/Records.java`:

```java
package com.shop.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Persistent shapes of the order aggregate. */
public final class Records {

    private Records() {
    }

    public record Order(UUID id, String customerRef, OrderStatus status, BigDecimal total,
                        String idempotencyKey, long version) {
    }

    public record OrderItem(UUID productId, int quantity, BigDecimal unitPrice) {
    }

    public record Saga(UUID id, SagaState state, String currentStep, long version, Instant updatedAt) {
    }

    /** Everything a decision needs, loaded at once. */
    public record Loaded(Saga saga, Order order, java.util.List<OrderItem> items) {
    }
}
```

Create `order-service/src/main/java/com/shop/order/domain/SagaMessages.java`:

```java
package com.shop.order.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** What the saga receives from the other services, and what it sends. Pure data. */
public final class SagaMessages {

    private SagaMessages() {
    }

    public static final String PRODUCT_COMMANDS = "product.commands";
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String SAGA_REPLIES = "order.saga.replies";
    public static final String ORDER_EVENTS = "order.events";

    public static final String STEP_VALIDATE_PRODUCTS = "ValidateProducts";
    public static final String STEP_RESERVE_STOCK = "ReserveStock";
    public static final String STEP_RELEASE_STOCK = "ReleaseStock";

    // ---- replies --------------------------------------------------------------------------------------

    public sealed interface Reply permits ProductsValidated, ProductsRejected, StockReserved, StockRejected,
            StockReleased {
    }

    /** Current prices per product, as answered by Product. */
    public record ProductsValidated(Map<UUID, BigDecimal> prices) implements Reply {
    }

    public record ProductsRejected(List<String> rejections) implements Reply {
    }

    public record StockReserved() implements Reply {
    }

    public record StockRejected(List<String> shortages) implements Reply {
    }

    public record StockReleased() implements Reply {
    }

    // ---- commands --------------------------------------------------------------------------------------

    public record Line(UUID productId, int quantity) {
    }

    public sealed interface Command permits ValidateProducts, ReserveStock, ReleaseStock {

        String topic();

        String type();

        Object data();
    }

    public record ValidateProducts(List<Line> items) implements Command {

        public String topic() {
            return PRODUCT_COMMANDS;
        }

        public String type() {
            return STEP_VALIDATE_PRODUCTS;
        }

        public Object data() {
            return Map.of("items", items);
        }
    }

    public record ReserveStock(List<Line> items) implements Command {

        public String topic() {
            return INVENTORY_COMMANDS;
        }

        public String type() {
            return STEP_RESERVE_STOCK;
        }

        public Object data() {
            return Map.of("items", items);
        }
    }

    public record ReleaseStock(String reason) implements Command {

        public String topic() {
            return INVENTORY_COMMANDS;
        }

        public String type() {
            return STEP_RELEASE_STOCK;
        }

        public Object data() {
            return Map.of("reason", reason);
        }
    }

    // ---- domain events on order.events ------------------------------------------------------------------

    public enum EventType { OrderCreated, OrderConfirmed, OrderRejected, OrderCancelled }
}
```

Create `order-service/src/main/java/com/shop/order/domain/Pricing.java`:

```java
package com.shop.order.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.shop.order.domain.Records.OrderItem;

/** Price snapshot of an order: the unit price of every item and the total. Pure. */
public record Pricing(Map<UUID, BigDecimal> unitPrices, BigDecimal total) {

    /** Empty if some item has no price in {@code prices} (the Product service should never do that). */
    public static Optional<Pricing> of(List<OrderItem> items, Map<UUID, BigDecimal> prices) {
        BigDecimal total = BigDecimal.ZERO;
        for (OrderItem item : items) {
            BigDecimal price = prices.get(item.productId());
            if (price == null) {
                return Optional.empty();
            }
            total = total.add(price.multiply(BigDecimal.valueOf(item.quantity())));
        }
        return Optional.of(new Pricing(Map.copyOf(prices), total));
    }
}
```

- [ ] **Step 4: Run it and see it pass**

```bash
cd order-service && mvn -B test -Dtest=PricingTest
```

Expected: `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add order-service
git commit -m "feat(order): domain vocabulary and price snapshot"
```

### Task 3: The saga state machine (pure)

**Files:**

- Create: `order-service/src/test/java/com/shop/order/domain/SagaMachineTest.java`
- Create: `order-service/src/main/java/com/shop/order/domain/SagaMachine.java`

- [ ] **Step 1: Write the failing test. It is the specification of the orchestrator: nominal path, both rejections without compensation, a missing price, cancellation of a confirmed order only, completion of the cancellation, the three timeout cases, finished sagas that do not time out, and replies that do not fit the state**

Create `order-service/src/test/java/com/shop/order/domain/SagaMachineTest.java`:

```java
package com.shop.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.Records.OrderItem;
import com.shop.order.domain.Records.Saga;
import com.shop.order.domain.SagaMachine.Apply;
import com.shop.order.domain.SagaMachine.Decision;
import com.shop.order.domain.SagaMachine.Direction;
import com.shop.order.domain.SagaMachine.Ignore;
import com.shop.order.domain.SagaMachine.Refuse;
import com.shop.order.domain.SagaMachine.Transition;
import com.shop.order.domain.SagaMessages.EventType;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.domain.SagaMessages.ProductsRejected;
import com.shop.order.domain.SagaMessages.ProductsValidated;
import com.shop.order.domain.SagaMessages.ReleaseStock;
import com.shop.order.domain.SagaMessages.ReserveStock;
import com.shop.order.domain.SagaMessages.StockRejected;
import com.shop.order.domain.SagaMessages.StockReleased;
import com.shop.order.domain.SagaMessages.StockReserved;
import org.junit.jupiter.api.Test;

class SagaMachineTest {

    private static final UUID ORDER = UUID.randomUUID();
    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    private static Loaded loaded(SagaState saga, OrderStatus order) {
        return new Loaded(
                new Saga(ORDER, saga, "X", 3, Instant.now()),
                new Order(ORDER, "cust-1", order, null, "key", 5),
                List.of(new OrderItem(A, 2, null), new OrderItem(B, 1, null)));
    }

    private static Transition applied(Decision decision) {
        assertThat(decision).isInstanceOf(Apply.class);
        return ((Apply) decision).transition();
    }

    // ---- nominal path ------------------------------------------------------------------------------

    @Test
    void productsValidatedPricesTheOrderAndAsksInventoryToReserve() {
        var prices = Map.of(A, new BigDecimal("10.00"), B, new BigDecimal("5.50"));

        Transition t = applied(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING),
                new ProductsValidated(prices)));

        assertThat(t.state()).isEqualTo(SagaState.PRODUCTS_VALIDATED);
        assertThat(t.orderStatus()).isEmpty();
        assertThat(t.command()).contains(new ReserveStock(List.of(new Line(A, 2), new Line(B, 1))));
        assertThat(t.pricing().orElseThrow().total()).isEqualByComparingTo("25.50");
        assertThat(t.steps()).singleElement().satisfies(s -> {
            assertThat(s.step()).isEqualTo("ValidateProducts");
            assertThat(s.direction()).isEqualTo(Direction.FORWARD);
            assertThat(s.outcome()).isEqualTo("VALIDATED");
        });
        assertThat(t.event()).isEmpty();
    }

    @Test
    void stockReservedConfirmsTheOrder() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING), new StockReserved()));

        assertThat(t.state()).isEqualTo(SagaState.COMPLETED);
        assertThat(t.orderStatus()).contains(OrderStatus.CONFIRMED);
        assertThat(t.event()).contains(EventType.OrderConfirmed);
        assertThat(t.command()).isEmpty();
    }

    // ---- failures ------------------------------------------------------------------------------------

    @Test
    void productsRejectedRejectsTheOrderWithoutCompensation() {
        Transition t = applied(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING),
                new ProductsRejected(List.of("NOT_FOUND"))));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.orderStatus()).contains(OrderStatus.REJECTED);
        assertThat(t.event()).contains(EventType.OrderRejected);
        assertThat(t.command()).isEmpty();
        assertThat(t.steps().get(0).detail()).isEqualTo(List.of("NOT_FOUND"));
    }

    @Test
    void stockRejectedRejectsTheOrderWithoutCompensation() {
        Transition t = applied(SagaMachine.onReply(loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING),
                new StockRejected(List.of("short"))));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.orderStatus()).contains(OrderStatus.REJECTED);
        assertThat(t.reason()).isEqualTo("STOCK_REJECTED");
        assertThat(t.command()).isEmpty();
    }

    @Test
    void aMissingPriceRejectsTheOrder() {
        Transition t = applied(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING),
                new ProductsValidated(Map.of(A, BigDecimal.ONE))));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.reason()).isEqualTo("PRICE_MISSING");
    }

    // ---- cancellation ----------------------------------------------------------------------------------

    @Test
    void cancelOfAConfirmedOrderStartsTheCompensation() {
        Transition t = applied(SagaMachine.cancel(loaded(SagaState.COMPLETED, OrderStatus.CONFIRMED)));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLING);
        assertThat(t.command()).contains(new ReleaseStock("ORDER_CANCELLED"));
        assertThat(t.steps().get(0).direction()).isEqualTo(Direction.COMPENSATE);
    }

    @Test
    void stockReleasedCompletesTheCancellation() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING), new StockReleased()));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATED);
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLED);
        assertThat(t.event()).contains(EventType.OrderCancelled);
    }

    @Test
    void onlyAConfirmedOrderCanBeCancelled() {
        for (OrderStatus status : List.of(OrderStatus.PENDING, OrderStatus.REJECTED, OrderStatus.CANCELLING,
                OrderStatus.CANCELLED)) {
            assertThat(SagaMachine.cancel(loaded(SagaState.STARTED, status))).isInstanceOf(Refuse.class);
        }
    }

    // ---- timeout -----------------------------------------------------------------------------------------

    @Test
    void aStartedSagaThatTimesOutIsRejected() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.STARTED, OrderStatus.PENDING)));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.reason()).isEqualTo("TIMEOUT");
        assertThat(t.event()).contains(EventType.OrderRejected);
        assertThat(t.command()).isEmpty();
    }

    @Test
    void aSagaWaitingForStockThatTimesOutIsCompensated() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING)));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLING);
        assertThat(t.command()).contains(new ReleaseStock("TIMEOUT"));
        assertThat(t.steps()).extracting(SagaMachine.StepEntry::outcome).containsExactly("TIMED_OUT", "REQUESTED");
    }

    @Test
    void aCompensationWithoutAnswerAsksAgainAndKeepsTheStatus() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING)));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.orderStatus()).isEqualTo(Optional.empty());
        assertThat(t.command()).contains(new ReleaseStock("TIMEOUT"));
    }

    @Test
    void finishedSagasDoNotTimeOut() {
        for (SagaState state : List.of(SagaState.COMPLETED, SagaState.REJECTED, SagaState.COMPENSATED)) {
            assertThat(SagaMachine.timeout(loaded(state, OrderStatus.CONFIRMED))).isInstanceOf(Ignore.class);
        }
    }

    // ---- duplicates and late replies -----------------------------------------------------------------------

    @Test
    void repliesThatDoNotFitTheStateAreIgnored() {
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPLETED, OrderStatus.CONFIRMED), new StockReserved()))
                .isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING), new StockReserved()))
                .isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING), new StockReserved()))
                .isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.REJECTED, OrderStatus.REJECTED),
                new ProductsValidated(Map.of(A, BigDecimal.ONE, B, BigDecimal.ONE)))).isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING), new StockReleased()))
                .isInstanceOf(Ignore.class);
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd order-service && mvn -B -q test -Dtest=SagaMachineTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... SagaMachine`.

- [ ] **Step 3: Implement. `onReply`, `cancel` and `timeout` each return a `Decision`: `Apply(Transition)`, `Ignore(why)` or `Refuse(why)`. A `Transition` is data: next saga state, optional order status change, optional command, optional event, history entries, optional price snapshot**

Create `order-service/src/main/java/com/shop/order/domain/SagaMachine.java`:

```java
package com.shop.order.domain;

import java.util.List;
import java.util.Optional;

import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.OrderItem;
import com.shop.order.domain.SagaMessages.Command;
import com.shop.order.domain.SagaMessages.EventType;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.domain.SagaMessages.ProductsRejected;
import com.shop.order.domain.SagaMessages.ProductsValidated;
import com.shop.order.domain.SagaMessages.ReleaseStock;
import com.shop.order.domain.SagaMessages.Reply;
import com.shop.order.domain.SagaMessages.ReserveStock;
import com.shop.order.domain.SagaMessages.StockRejected;
import com.shop.order.domain.SagaMessages.StockReleased;
import com.shop.order.domain.SagaMessages.StockReserved;

/**
 * The saga orchestrator's brain: a set of pure functions from (current order and saga, what happened)
 * to a {@link Decision}. No database, no Kafka, no clock. The coordinator loads the state, calls one of
 * these functions, and applies the resulting {@link Transition} in a single transaction.
 *
 * <pre>
 * STARTED --ProductsValidated--> PRODUCTS_VALIDATED --StockReserved--> COMPLETED --cancel--> COMPENSATING --StockReleased--> COMPENSATED
 *    |                                  |                                                           (order CANCELLING -> CANCELLED)
 *    +--ProductsRejected--> REJECTED    +--StockRejected--> REJECTED
 * timeout: STARTED -> REJECTED, PRODUCTS_VALIDATED -> COMPENSATING, COMPENSATING -> ReleaseStock sent again
 * </pre>
 */
public final class SagaMachine {

    private SagaMachine() {
    }

    public enum Direction { FORWARD, COMPENSATE }

    /** One line of {@code saga_step_history}. */
    public record StepEntry(String step, Direction direction, String outcome, Object detail) {
    }

    /** Everything that must change, as data. */
    public record Transition(
            SagaState state,
            String currentStep,
            Optional<OrderStatus> orderStatus,
            String reason,
            Optional<Command> command,
            Optional<EventType> event,
            List<StepEntry> steps,
            Optional<Pricing> pricing) {
    }

    public sealed interface Decision permits Apply, Ignore, Refuse {
    }

    /** Apply this transition. */
    public record Apply(Transition transition) implements Decision {
    }

    /** The message does not fit the current state (duplicate, late): do nothing. */
    public record Ignore(String why) implements Decision {
    }

    /** The request is not allowed in the current state (reported to the caller). */
    public record Refuse(String why) implements Decision {
    }

    // ---- a reply from Product or Inventory -----------------------------------------------------------

    public static Decision onReply(Loaded ctx, Reply reply) {
        SagaState state = ctx.saga().state();
        return switch (reply) {
            case ProductsValidated r when state == SagaState.STARTED -> productsValidated(ctx, r);
            case ProductsRejected r when state == SagaState.STARTED ->
                    rejected("PRODUCT_REJECTED", SagaMessages.STEP_VALIDATE_PRODUCTS, "REJECTED", r.rejections());
            case StockReserved r when state == SagaState.PRODUCTS_VALIDATED -> new Apply(new Transition(
                    SagaState.COMPLETED, SagaMessages.STEP_RESERVE_STOCK,
                    Optional.of(OrderStatus.CONFIRMED), "STOCK_RESERVED",
                    Optional.empty(), Optional.of(EventType.OrderConfirmed),
                    List.of(new StepEntry(SagaMessages.STEP_RESERVE_STOCK, Direction.FORWARD, "RESERVED", null)),
                    Optional.empty()));
            case StockRejected r when state == SagaState.PRODUCTS_VALIDATED ->
                    rejected("STOCK_REJECTED", SagaMessages.STEP_RESERVE_STOCK, "REJECTED", r.shortages());
            case StockReleased r when state == SagaState.COMPENSATING -> new Apply(new Transition(
                    SagaState.COMPENSATED, SagaMessages.STEP_RELEASE_STOCK,
                    Optional.of(OrderStatus.CANCELLED), "STOCK_RELEASED",
                    Optional.empty(), Optional.of(EventType.OrderCancelled),
                    List.of(new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "RELEASED", null)),
                    Optional.empty()));
            default -> new Ignore(reply.getClass().getSimpleName() + " is not expected in saga state " + state);
        };
    }

    private static Decision productsValidated(Loaded ctx, ProductsValidated reply) {
        Optional<Pricing> pricing = Pricing.of(ctx.items(), reply.prices());
        if (pricing.isEmpty()) {
            return rejected("PRICE_MISSING", SagaMessages.STEP_VALIDATE_PRODUCTS, "REJECTED",
                    "Product service answered without a price for every item");
        }
        return new Apply(new Transition(
                SagaState.PRODUCTS_VALIDATED, SagaMessages.STEP_RESERVE_STOCK,
                Optional.empty(), "PRODUCTS_VALIDATED",
                Optional.of(new ReserveStock(lines(ctx.items()))), Optional.empty(),
                List.of(new StepEntry(SagaMessages.STEP_VALIDATE_PRODUCTS, Direction.FORWARD, "VALIDATED", null)),
                pricing));
    }

    // ---- cancellation requested by the customer -------------------------------------------------------

    /** Only a confirmed order can be cancelled; that starts the compensation. */
    public static Decision cancel(Loaded ctx) {
        if (ctx.order().status() != OrderStatus.CONFIRMED || ctx.saga().state() != SagaState.COMPLETED) {
            return new Refuse("Order is " + ctx.order().status() + "; only a CONFIRMED order can be cancelled");
        }
        return new Apply(new Transition(
                SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                Optional.of(OrderStatus.CANCELLING), "CANCEL_REQUESTED",
                Optional.of(new ReleaseStock("ORDER_CANCELLED")), Optional.empty(),
                List.of(new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "REQUESTED", null)),
                Optional.empty()));
    }

    // ---- the saga made no progress for too long -------------------------------------------------------

    public static Decision timeout(Loaded ctx) {
        return switch (ctx.saga().state()) {
            // nothing was reserved yet: reject
            case STARTED -> rejected("TIMEOUT", SagaMessages.STEP_VALIDATE_PRODUCTS, "TIMED_OUT",
                    "No answer from the Product service");
            // ReserveStock was sent, a reservation may exist: compensate
            case PRODUCTS_VALIDATED -> new Apply(new Transition(
                    SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                    Optional.of(OrderStatus.CANCELLING), "TIMEOUT",
                    Optional.of(new ReleaseStock("TIMEOUT")), Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_RESERVE_STOCK, Direction.FORWARD, "TIMED_OUT", null),
                            new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "REQUESTED", null)),
                    Optional.empty()));
            // the release got no answer: ask again (Inventory's release is idempotent)
            case COMPENSATING -> new Apply(new Transition(
                    SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                    Optional.empty(), "RELEASE_RETRIED",
                    Optional.of(new ReleaseStock("TIMEOUT")), Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "RETRIED", null)),
                    Optional.empty()));
            case COMPLETED, REJECTED, COMPENSATED -> new Ignore("Saga is " + ctx.saga().state() + ", nothing to time out");
        };
    }

    // ---- helpers ----------------------------------------------------------------------------------------

    private static Decision rejected(String reason, String step, String outcome, Object detail) {
        return new Apply(new Transition(
                SagaState.REJECTED, step,
                Optional.of(OrderStatus.REJECTED), reason,
                Optional.empty(), Optional.of(EventType.OrderRejected),
                List.of(new StepEntry(step, Direction.FORWARD, outcome, detail)),
                Optional.empty()));
    }

    private static List<Line> lines(List<OrderItem> items) {
        return items.stream().map(i -> new Line(i.productId(), i.quantity())).toList();
    }
}
```

- [ ] **Step 4: Run it and see it pass**

```bash
cd order-service && mvn -B test -Dtest=SagaMachineTest
```

Expected: `Tests run: 13, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add order-service
git commit -m "feat(order): pure saga state machine"
```

### Task 4: Request validation and DTOs

**Files:**

- Create: `order-service/src/test/java/com/shop/order/web/RequestValidationTest.java`
- Create: `order-service/src/main/java/com/shop/order/web/{OrderDtos,RequestValidation}.java`

- [ ] **Step 1: Write the failing test**

Create `order-service/src/test/java/com/shop/order/web/RequestValidationTest.java`:

```java
package com.shop.order.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import com.shop.order.web.OrderDtos.CreateOrderRequest;
import com.shop.order.web.OrderDtos.ItemRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    private static CreateOrderRequest request(String customer, ItemRequest... items) {
        return new CreateOrderRequest(customer, List.of(items));
    }

    @Test
    void acceptsAValidRequest() {
        assertThat(RequestValidation.check("key-1", request("cust", new ItemRequest(A, 2), new ItemRequest(B, 1))))
                .isEmpty();
    }

    @Test
    void requiresTheIdempotencyKey() {
        assertThat(RequestValidation.check(null, request("cust", new ItemRequest(A, 1))))
                .containsExactly("Idempotency-Key header is required");
        assertThat(RequestValidation.check(" ", request("cust", new ItemRequest(A, 1)))).hasSize(1);
        assertThat(RequestValidation.check("k".repeat(101), request("cust", new ItemRequest(A, 1)))).hasSize(1);
    }

    @Test
    void requiresACustomerAndAtLeastOneItem() {
        assertThat(RequestValidation.check("k", new CreateOrderRequest(" ", null)))
                .containsExactly("customerRef is required", "items must contain at least one item");
        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", List.of())))
                .containsExactly("items must contain at least one item");
    }

    @Test
    void checksEveryItem() {
        assertThat(RequestValidation.check("k", request("c",
                new ItemRequest(null, 1), new ItemRequest(A, 0), new ItemRequest(B, 10_001), new ItemRequest(A, null))))
                .containsExactly(
                        "items[0].productId is required",
                        "items[1].quantity must be between 1 and 10000",
                        "items[2].quantity must be between 1 and 10000",
                        "items[3].productId appears more than once",
                        "items[3].quantity must be between 1 and 10000");
    }

    @Test
    void limitsTheNumberOfItems() {
        List<ItemRequest> many = java.util.stream.IntStream.range(0, 101)
                .mapToObj(i -> new ItemRequest(new UUID(0, i), 1)).toList();

        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", many)))
                .containsExactly("items must contain at most 100 items");
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd order-service && mvn -B -q test -Dtest=RequestValidationTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... RequestValidation`.

- [ ] **Step 3: Implement**

Create `order-service/src/main/java/com/shop/order/web/OrderDtos.java`:

```java
package com.shop.order.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.shop.order.domain.OrderStatus;
import tools.jackson.databind.JsonNode;

/** Wire format of the HTTP API. */
public final class OrderDtos {

    private OrderDtos() {
    }

    public record CreateOrderRequest(String customerRef, List<ItemRequest> items) {
    }

    public record ItemRequest(UUID productId, Integer quantity) {
    }

    public record AcceptedResponse(UUID orderId, OrderStatus status) {
    }

    public record ItemResponse(UUID productId, int quantity, BigDecimal unitPrice) {
    }

    /** {@code total} and {@code unitPrice} are null until the Product service has answered. */
    public record OrderResponse(UUID id, String customerRef, OrderStatus status, BigDecimal total,
                                List<ItemResponse> items, long version) {
    }

    public record StatusEntryResponse(OrderStatus from, OrderStatus to, String reason, UUID triggerMessageId,
                                      Instant occurredAt) {
    }

    public record StepEntryResponse(String step, String direction, String outcome, UUID messageId, JsonNode detail,
                                    Instant occurredAt) {
    }

    public record HistoryResponse(UUID orderId, List<StatusEntryResponse> status, List<StepEntryResponse> steps) {
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
```

Create `order-service/src/main/java/com/shop/order/web/RequestValidation.java`:

```java
package com.shop.order.web;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.shop.order.web.OrderDtos.CreateOrderRequest;
import com.shop.order.web.OrderDtos.ItemRequest;

/** Pure request checks: the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_ITEMS = 100;
    static final int MAX_QUANTITY = 10_000;
    static final int MAX_TEXT = 100;

    private RequestValidation() {
    }

    static List<String> check(String idempotencyKey, CreateOrderRequest request) {
        List<String> errors = new ArrayList<>();
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            errors.add("Idempotency-Key header is required");
        } else if (idempotencyKey.length() > MAX_TEXT) {
            errors.add("Idempotency-Key must be at most " + MAX_TEXT + " characters");
        }
        if (request.customerRef() == null || request.customerRef().isBlank()) {
            errors.add("customerRef is required");
        } else if (request.customerRef().length() > MAX_TEXT) {
            errors.add("customerRef must be at most " + MAX_TEXT + " characters");
        }
        List<ItemRequest> items = request.items();
        if (items == null || items.isEmpty()) {
            errors.add("items must contain at least one item");
            return errors;
        }
        if (items.size() > MAX_ITEMS) {
            errors.add("items must contain at most " + MAX_ITEMS + " items");
        }
        Set<UUID> seen = new HashSet<>();
        for (int i = 0; i < items.size(); i++) {
            ItemRequest item = items.get(i);
            if (item == null || item.productId() == null) {
                errors.add("items[" + i + "].productId is required");
                continue;
            }
            if (!seen.add(item.productId())) {
                errors.add("items[" + i + "].productId appears more than once");
            }
            if (item.quantity() == null || item.quantity() < 1 || item.quantity() > MAX_QUANTITY) {
                errors.add("items[" + i + "].quantity must be between 1 and " + MAX_QUANTITY);
            }
        }
        return errors;
    }
}
```

- [ ] **Step 4: Run all unit tests**

```bash
cd order-service && mvn -B test
```

Expected: `Tests run: 20, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add order-service
git commit -m "feat(order): order request DTOs and validation"
```

### Task 5: Orders API integration test (written first)

**Files:**

- Create: `order-service/src/test/java/com/shop/order/support/{PostgresSupport,KafkaSupport,KafkaTestClient,SagaTestBase}.java`
- Create: `order-service/src/test/java/com/shop/order/OrderApiIT.java`

- [ ] **Step 1: Create the container support. `KafkaTestClient.createTopics` skips topics that already exist, because several test classes share one broker**

Create `order-service/src/test/java/com/shop/order/support/PostgresSupport.java`:

```java
package com.shop.order.support;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.postgresql.PostgreSQLContainer;

/** One PostgreSQL container shared by every integration test of the JVM (stopped by Testcontainers at exit). */
public final class PostgresSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("order_it")
            .withUsername("shop")
            .withPassword("shop");

    static {
        POSTGRES.start();
    }

    private PostgresSupport() {
    }

    /** {@code shop.r2dbc.*} properties pointing at the container, with a small pool. */
    public static Map<String, String> r2dbcProperties() {
        return Stream.of(
                        new String[] {"shop.r2dbc.host", POSTGRES.getHost()},
                        new String[] {"shop.r2dbc.port", String.valueOf(POSTGRES.getMappedPort(5432))},
                        new String[] {"shop.r2dbc.database", POSTGRES.getDatabaseName()},
                        new String[] {"shop.r2dbc.username", POSTGRES.getUsername()},
                        new String[] {"shop.r2dbc.password", POSTGRES.getPassword()},
                        new String[] {"shop.r2dbc.pool.initial-size", "1"},
                        new String[] {"shop.r2dbc.pool.max-size", "5"})
                .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }
}
```

Create `order-service/src/test/java/com/shop/order/support/KafkaSupport.java`:

```java
package com.shop.order.support;

import org.testcontainers.kafka.KafkaContainer;

/** One Kafka broker (KRaft) shared by every integration test of the JVM. */
public final class KafkaSupport {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static {
        KAFKA.start();
    }

    private KafkaSupport() {
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
```

Create `order-service/src/test/java/com/shop/order/support/KafkaTestClient.java`:

```java
package com.shop.order.support;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Minimal Kafka client for tests: create topics, publish like Debezium does, read a topic. */
public final class KafkaTestClient {

    private final String bootstrapServers;

    public KafkaTestClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /** Creates the topics that do not exist yet (several test classes share one broker). */
    public void createTopics(String... topics) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            java.util.Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                if (!existing.contains(topic)) {
                    newTopics.add(new NewTopic(topic, 1, (short) 1));
                }
            }
            if (!newTopics.isEmpty()) {
                admin.createTopics(newTopics).all().get();
            }
        }
    }

    /** Publishes a record with the {@code id} and {@code type} headers that Debezium's EventRouter sets. */
    public void send(String topic, String key, UUID id, String type, String payload) throws Exception {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
            record.headers().add("id", id.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("type", type.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }
    }

    /** Reads {@code topic} from the beginning until {@code expected} records arrived or the timeout elapsed. */
    public List<ConsumerRecord<String, String>> read(String topic, int expected, Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
        }
        return records;
    }

    /** Reads {@code topic} from the beginning until a record matches {@code wanted} or the timeout elapsed. */
    public java.util.Optional<ConsumerRecord<String, String>> readUntil(
            String topic, java.util.function.Predicate<ConsumerRecord<String, String>> wanted, Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (wanted.test(record)) {
                        return java.util.Optional.of(record);
                    }
                }
            }
        }
        return java.util.Optional.empty();
    }
}
```

- [ ] **Step 2: Create the shared test base: HTTP helpers, reading what the service wrote to its outbox, and building the replies of the other services**

Create `order-service/src/test/java/com/shop/order/support/SagaTestBase.java`:

```java
package com.shop.order.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;

/** Helpers shared by the integration tests: talk to the API, read the outbox, play the other services. */
public abstract class SagaTestBase {

    public static final String REPLIES = "order.saga.replies";

    @Autowired
    protected WebTestClient client;

    @Autowired
    protected DatabaseClient db;

    @Autowired
    protected JsonMapper json;

    @BeforeEach
    protected void cleanDatabase() {
        db.sql("TRUNCATE orders, order_items, saga_state, order_status_history, saga_step_history, "
                        + "outbox_events, processed_events CASCADE")
                .fetch().rowsUpdated().block();
    }

    // ---- the HTTP API ----------------------------------------------------------------------------------

    protected static String orderBody(UUID... productsOfQuantityTwo) {
        StringBuilder items = new StringBuilder();
        for (UUID p : productsOfQuantityTwo) {
            if (items.length() > 0) {
                items.append(',');
            }
            items.append("{\"productId\":\"%s\",\"quantity\":2}".formatted(p));
        }
        return "{\"customerRef\":\"cust-1\",\"items\":[" + items + "]}";
    }

    protected UUID createOrder(UUID... products) {
        return createOrder(UUID.randomUUID().toString(), orderBody(products));
    }

    protected UUID createOrder(String idempotencyKey, String body) {
        String response = client.post().uri("/orders")
                .header("Idempotency-Key", idempotencyKey)
                .header("Content-Type", "application/json")
                .bodyValue(body)
                .exchange()
                .expectStatus().isAccepted()
                .returnResult(String.class).getResponseBody().blockFirst();
        return UUID.fromString(json.readTree(response).get("orderId").asString());
    }

    protected JsonNode order(UUID orderId) {
        String response = client.get().uri("/orders/{id}", orderId)
                .exchange().expectStatus().isOk()
                .returnResult(String.class).getResponseBody().blockFirst();
        return json.readTree(response);
    }

    protected JsonNode history(UUID orderId) {
        String response = client.get().uri("/orders/{id}/history", orderId)
                .exchange().expectStatus().isOk()
                .returnResult(String.class).getResponseBody().blockFirst();
        return json.readTree(response);
    }

    protected HttpStatus cancel(UUID orderId) {
        return HttpStatus.valueOf(client.post().uri("/orders/{id}/cancel", orderId)
                .exchange().returnResult(String.class).getStatus().value());
    }

    protected String statusOf(UUID orderId) {
        return order(orderId).get("status").asString();
    }

    protected void awaitStatus(UUID orderId, String expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(statusOf(orderId)).isEqualTo(expected));
    }

    // ---- the outbox: what the order service sent -------------------------------------------------------

    protected List<Map<String, Object>> outbox(UUID orderId, String type) {
        return db.sql("SELECT topic, aggregateid, type, payload::text AS payload FROM outbox_events "
                        + "WHERE aggregateid = :id AND type = :type ORDER BY occurred_at, id")
                .bind("id", orderId.toString()).bind("type", type)
                .fetch().all().collectList().block();
    }

    protected Map<String, Object> awaitOutbox(UUID orderId, String type) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> !outbox(orderId, type).isEmpty());
        return outbox(orderId, type).get(0);
    }

    protected JsonNode payload(Map<String, Object> outboxRow) {
        return json.readTree((String) outboxRow.get("payload"));
    }

    // ---- the other services: replies as Debezium would deliver them -------------------------------------

    protected static String reply(UUID orderId, String dataJson) {
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"%s\",\"data\":%s}"
                .formatted(orderId, orderId, REPLIES, dataJson);
    }

    protected static String productsValidated(UUID orderId, Map<UUID, String> prices) {
        StringBuilder items = new StringBuilder();
        prices.forEach((p, price) -> {
            if (items.length() > 0) {
                items.append(',');
            }
            items.append("{\"productId\":\"%s\",\"unitPrice\":%s}".formatted(p, price));
        });
        return reply(orderId, "{\"items\":[" + items + "]}");
    }
}
```

- [ ] **Step 3: Create the API test: one transaction creates order, saga, both histories and the outbox rows; the same idempotency key returns the same order, also when 8 requests race; every validation error is listed; 404 and 400; a non-confirmed order cannot be cancelled. `shop.kafka.bootstrap-servers=false` turns the Kafka beans off, so this test needs no broker**

Create `order-service/src/test/java/com/shop/order/OrderApiIT.java`:

```java
package com.shop.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import com.shop.order.support.PostgresSupport;
import com.shop.order.support.SagaTestBase;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The HTTP side against a real PostgreSQL: one transaction creates order, saga, histories and outbox rows. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class OrderApiIT extends SagaTestBase {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        // "false" switches the Kafka beans off (@ConditionalOnProperty), so this test needs no broker
        registry.add("shop.kafka.bootstrap-servers", () -> "false");
    }

    private final UUID a = UUID.randomUUID();
    private final UUID b = UUID.randomUUID();

    @Test
    void creatingAnOrderStartsTheSagaInOneTransaction() {
        UUID orderId = createOrder(a, b);

        var order = order(orderId);
        assertThat(order.get("status").asString()).isEqualTo("PENDING");
        assertThat(order.get("total").isNull()).isTrue();
        assertThat(order.get("items")).hasSize(2);

        // the command for the Product service and the domain event, both in the outbox
        var command = awaitOutbox(orderId, "ValidateProducts");
        assertThat(command.get("topic")).isEqualTo("product.commands");
        var data = payload(command);
        assertThat(data.get("sagaId").asString()).isEqualTo(orderId.toString());
        assertThat(data.get("replyTo").asString()).isEqualTo("order.saga.replies");
        assertThat(data.get("data").get("items")).hasSize(2);
        assertThat(data.get("data").get("items").get(0).get("quantity").asInt()).isEqualTo(2);

        var event = awaitOutbox(orderId, "OrderCreated");
        assertThat(event.get("topic")).isEqualTo("order.events");

        // both histories
        var history = history(orderId);
        assertThat(history.get("status")).hasSize(1);
        assertThat(history.get("status").get(0).get("from").isNull()).isTrue();
        assertThat(history.get("status").get(0).get("to").asString()).isEqualTo("PENDING");
        assertThat(history.get("steps")).hasSize(1);
        assertThat(history.get("steps").get(0).get("step").asString()).isEqualTo("ValidateProducts");
        assertThat(history.get("steps").get(0).get("outcome").asString()).isEqualTo("REQUESTED");
    }

    @Test
    void theSameIdempotencyKeyReturnsTheSameOrder() {
        String body = orderBody(a);

        UUID first = createOrder("key-1", body);
        UUID second = createOrder("key-1", body);

        assertThat(second).isEqualTo(first);
        assertThat(db.sql("SELECT COUNT(*) FROM orders").map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
        assertThat(outbox(first, "ValidateProducts")).hasSize(1);
    }

    @Test
    void identicalRequestsRacingCreateOneOrder() {
        String body = orderBody(a);

        List<UUID> ids = IntStream.range(0, 8).parallel()
                .mapToObj(i -> createOrder("race-key", body)).toList();

        assertThat(ids).containsOnly(ids.get(0));
        assertThat(db.sql("SELECT COUNT(*) FROM orders").map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
        assertThat(db.sql("SELECT COUNT(*) FROM outbox_events WHERE type = 'ValidateProducts'")
                .map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidRequestsWithEveryViolation() {
        client.post().uri("/orders")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerRef\":\"\",\"items\":[]}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo("INVALID_REQUEST")
                .jsonPath("$.details.length()").isEqualTo(3);
        assertThat(db.sql("SELECT COUNT(*) FROM orders").map(r -> r.get(0, Long.class)).one().block()).isZero();
    }

    @Test
    void unknownOrderIsNotFoundAndMalformedIdIsBadRequest() {
        client.get().uri("/orders/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("ORDER_NOT_FOUND");
        client.get().uri("/orders/{id}/history", UUID.randomUUID()).exchange().expectStatus().isNotFound();
        client.get().uri("/orders/not-a-uuid").exchange().expectStatus().isBadRequest();
    }

    @Test
    void anOrderThatIsNotConfirmedCannotBeCancelled() {
        UUID orderId = createOrder(a);

        assertThat(cancel(orderId)).isEqualTo(HttpStatus.CONFLICT);
        assertThat(cancel(UUID.randomUUID())).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(statusOf(orderId)).isEqualTo("PENDING");
    }
}
```

- [ ] **Step 4: Run it and see it fail. Docker must be running; the first run pulls `postgres:18-alpine` and `apache/kafka:4.1.0`**

```bash
cd order-service && mvn -B verify -Dit.test=OrderApiIT
```

Expected: tests fail (404: there are no routes yet). `Could not find a valid Docker environment` means Docker is not running.

- [ ] **Step 5: Commit the failing test**

```bash
git add order-service
git commit -m "test(order): orders API integration test"
```

### Task 6: Repository, coordinator and the orders API

**Files:**

- Create: `order-service/src/main/java/com/shop/order/domain/OrderExceptions.java`
- Create: `order-service/src/main/java/com/shop/order/repository/OrderRepository.java`
- Create: `order-service/src/main/java/com/shop/order/service/{SagaOutbox,SagaCoordinator,OrderService}.java`
- Create: `order-service/src/main/java/com/shop/order/web/{InvalidRequestException,OrderHandler,OrderRoutes}.java`

- [ ] **Step 1: Create the domain exceptions**

Create `order-service/src/main/java/com/shop/order/domain/OrderExceptions.java`:

```java
package com.shop.order.domain;

import java.util.UUID;

public final class OrderExceptions {

    private OrderExceptions() {
    }

    public static class OrderNotFoundException extends RuntimeException {
        public OrderNotFoundException(UUID id) {
            super("Order " + id + " not found");
        }
    }

    /** The request contradicts the current state of the order (for example cancelling an unconfirmed order). */
    public static class OrderConflictException extends RuntimeException {
        public OrderConflictException(String message) {
            super(message);
        }
    }

    /**
     * Someone else changed the saga or the order between our read and our write. Transient: the whole
     * message transaction rolls back and the Kafka consumer replays it, or the caller retries.
     */
    public static class ConcurrentUpdateException extends RuntimeException {
        public ConcurrentUpdateException(String what, UUID id) {
            super(what + " " + id + " was modified concurrently");
        }
    }
}
```

- [ ] **Step 2: Create the repository: all SQL, optimistic updates (`WHERE version = :expected`, zero rows means `ConcurrentUpdateException`), and the stale-saga query used later by the sweeper**

Create `order-service/src/main/java/com/shop/order/repository/OrderRepository.java`:

```java
package com.shop.order.repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.domain.OrderExceptions.ConcurrentUpdateException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.Records.OrderItem;
import com.shop.order.domain.Records.Saga;
import com.shop.order.domain.SagaMachine.StepEntry;
import com.shop.order.domain.SagaState;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL of the order aggregate. {@code orders} and {@code saga_state} are updated with optimistic
 * locking ({@code WHERE version = :expected}): a lost race raises {@link ConcurrentUpdateException}
 * instead of overwriting.
 */
@Repository
public class OrderRepository {

    private static final String ORDER_COLUMNS = "id, customer_ref, status, total, idempotency_key, version";

    private final DatabaseClient db;

    public OrderRepository(DatabaseClient db) {
        this.db = db;
    }

    public record StatusHistoryRow(OrderStatus from, OrderStatus to, String reason, UUID triggerMessageId,
                                   Instant occurredAt) {
    }

    public record StepHistoryRow(String step, String direction, String outcome, UUID messageId, String detail,
                                 Instant occurredAt) {
    }

    // ---- create -------------------------------------------------------------------------------------------

    public Mono<Void> insertOrder(UUID id, String customerRef, String idempotencyKey) {
        return db.sql("""
                        INSERT INTO orders (id, customer_ref, status, idempotency_key)
                        VALUES (:id, :customerRef, 'PENDING', :key)""")
                .bind("id", id)
                .bind("customerRef", customerRef)
                .bind("key", idempotencyKey)
                .fetch().rowsUpdated().then();
    }

    public Mono<Void> insertItem(UUID orderId, UUID productId, int quantity) {
        return db.sql("INSERT INTO order_items (order_id, product_id, quantity) VALUES (:o, :p, :q)")
                .bind("o", orderId).bind("p", productId).bind("q", quantity)
                .fetch().rowsUpdated().then();
    }

    public Mono<Void> insertSaga(UUID orderId, SagaState state, String step) {
        return db.sql("INSERT INTO saga_state (saga_id, state, current_step) VALUES (:id, :state, :step)")
                .bind("id", orderId).bind("state", state.name()).bind("step", step)
                .fetch().rowsUpdated().then();
    }

    // ---- read ---------------------------------------------------------------------------------------------

    public Mono<Order> findOrder(UUID id) {
        return db.sql("SELECT " + ORDER_COLUMNS + " FROM orders WHERE id = :id")
                .bind("id", id).map(OrderRepository::toOrder).one();
    }

    public Mono<Order> findOrderByIdempotencyKey(String key) {
        return db.sql("SELECT " + ORDER_COLUMNS + " FROM orders WHERE idempotency_key = :key")
                .bind("key", key).map(OrderRepository::toOrder).one();
    }

    public Flux<OrderItem> findItems(UUID orderId) {
        return db.sql("SELECT product_id, quantity, unit_price FROM order_items WHERE order_id = :o ORDER BY product_id")
                .bind("o", orderId)
                .map(row -> new OrderItem(
                        row.get("product_id", UUID.class), row.get("quantity", Integer.class),
                        row.get("unit_price", BigDecimal.class)))
                .all();
    }

    public Mono<Saga> findSaga(UUID id) {
        return db.sql("SELECT saga_id, state, current_step, version, updated_at FROM saga_state WHERE saga_id = :id")
                .bind("id", id).map(OrderRepository::toSaga).one();
    }

    /** Order, items and saga in one go, empty if the order does not exist. */
    public Mono<Loaded> load(UUID orderId) {
        return findOrder(orderId).flatMap(order -> findSaga(orderId).zipWith(findItems(orderId).collectList())
                .map(t -> new Loaded(t.getT1(), order, t.getT2())));
    }

    /** Sagas that made no progress since {@code cutoff}, oldest first. */
    public Flux<UUID> findStaleSagas(Instant cutoff, int limit) {
        return db.sql("""
                        SELECT saga_id FROM saga_state
                         WHERE state IN ('STARTED', 'PRODUCTS_VALIDATED', 'COMPENSATING') AND updated_at < :cutoff
                         ORDER BY updated_at LIMIT :limit""")
                .bind("cutoff", OffsetDateTime.ofInstant(cutoff, java.time.ZoneOffset.UTC))
                .bind("limit", limit)
                .map(row -> row.get("saga_id", UUID.class))
                .all();
    }

    public Flux<StatusHistoryRow> statusHistory(UUID orderId) {
        return db.sql("""
                        SELECT from_status, to_status, reason, trigger_message_id, occurred_at
                          FROM order_status_history WHERE order_id = :o ORDER BY id""")
                .bind("o", orderId)
                .map(row -> new StatusHistoryRow(
                        row.get("from_status", String.class) == null ? null
                                : OrderStatus.valueOf(row.get("from_status", String.class)),
                        OrderStatus.valueOf(row.get("to_status", String.class)),
                        row.get("reason", String.class),
                        row.get("trigger_message_id", UUID.class),
                        row.get("occurred_at", OffsetDateTime.class).toInstant()))
                .all();
    }

    public Flux<StepHistoryRow> stepHistory(UUID sagaId) {
        return db.sql("""
                        SELECT step, direction, outcome, message_id, detail::text AS detail, occurred_at
                          FROM saga_step_history WHERE saga_id = :s ORDER BY id""")
                .bind("s", sagaId)
                .map(row -> new StepHistoryRow(
                        row.get("step", String.class), row.get("direction", String.class),
                        row.get("outcome", String.class), row.get("message_id", UUID.class),
                        row.get("detail", String.class), row.get("occurred_at", OffsetDateTime.class).toInstant()))
                .all();
    }

    // ---- write (optimistic) ---------------------------------------------------------------------------------

    public Mono<Void> updateSaga(Saga saga, SagaState newState, String newStep) {
        return db.sql("""
                        UPDATE saga_state SET state = :state, current_step = :step, version = version + 1, updated_at = now()
                         WHERE saga_id = :id AND version = :version""")
                .bind("state", newState.name()).bind("step", newStep)
                .bind("id", saga.id()).bind("version", saga.version())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentUpdateException("Saga", saga.id())));
    }

    public Mono<Void> updateOrderStatus(Order order, OrderStatus newStatus) {
        return db.sql("""
                        UPDATE orders SET status = :status, version = version + 1, updated_at = now()
                         WHERE id = :id AND version = :version""")
                .bind("status", newStatus.name()).bind("id", order.id()).bind("version", order.version())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentUpdateException("Order", order.id())));
    }

    /** Stores the price snapshot: unit prices on the items, total on the order. */
    public Mono<Void> applyPricing(Order order, Map<UUID, BigDecimal> unitPrices, BigDecimal total) {
        return Flux.fromIterable(unitPrices.entrySet())
                .concatMap(e -> db.sql("UPDATE order_items SET unit_price = :price WHERE order_id = :o AND product_id = :p")
                        .bind("price", e.getValue()).bind("o", order.id()).bind("p", e.getKey())
                        .fetch().rowsUpdated())
                .then(db.sql("""
                                UPDATE orders SET total = :total, version = version + 1, updated_at = now()
                                 WHERE id = :id AND version = :version""")
                        .bind("total", total).bind("id", order.id()).bind("version", order.version())
                        .fetch().rowsUpdated())
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentUpdateException("Order", order.id())));
    }

    // ---- histories (append only) ------------------------------------------------------------------------------

    public Mono<Void> insertStatusHistory(UUID orderId, OrderStatus from, OrderStatus to, String reason,
                                          UUID triggerMessageId) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO order_status_history (order_id, from_status, to_status, reason, trigger_message_id)
                        VALUES (:o, :from, :to, :reason, :trigger)""")
                .bind("o", orderId).bind("to", to.name()).bind("reason", reason);
        spec = from == null ? spec.bindNull("from", String.class) : spec.bind("from", from.name());
        spec = triggerMessageId == null ? spec.bindNull("trigger", UUID.class) : spec.bind("trigger", triggerMessageId);
        return spec.fetch().rowsUpdated().then();
    }

    public Mono<Void> insertStepHistory(UUID sagaId, StepEntry entry, UUID messageId, String detailJson) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO saga_step_history (saga_id, step, direction, outcome, message_id, detail)
                        VALUES (:s, :step, :direction, :outcome, :message, CAST(:detail AS jsonb))""")
                .bind("s", sagaId).bind("step", entry.step())
                .bind("direction", entry.direction().name()).bind("outcome", entry.outcome());
        spec = messageId == null ? spec.bindNull("message", UUID.class) : spec.bind("message", messageId);
        spec = detailJson == null ? spec.bindNull("detail", String.class) : spec.bind("detail", detailJson);
        return spec.fetch().rowsUpdated().then();
    }

    // ---- mapping ------------------------------------------------------------------------------------------------

    private static Order toOrder(Readable row) {
        return new Order(
                row.get("id", UUID.class), row.get("customer_ref", String.class),
                OrderStatus.valueOf(row.get("status", String.class)), row.get("total", BigDecimal.class),
                row.get("idempotency_key", String.class), row.get("version", Long.class));
    }

    private static Saga toSaga(Readable row) {
        return new Saga(
                row.get("saga_id", UUID.class), SagaState.valueOf(row.get("state", String.class)),
                row.get("current_step", String.class), row.get("version", Long.class),
                row.get("updated_at", OffsetDateTime.class).toInstant());
    }
}
```

- [ ] **Step 3: Create the outbox helper (commands and domain events, key = order id) and the coordinator. `apply` is the only place where a `Transition` becomes writes: saga row, order status and its history, price snapshot, step history, command, event**

Create `order-service/src/main/java/com/shop/order/service/SagaOutbox.java`:

```java
package com.shop.order.service;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.outbox.OutboxWriter;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.SagaMessages;
import com.shop.order.domain.SagaMessages.Command;
import com.shop.order.domain.SagaMessages.EventType;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;

/** Writes the saga's outgoing messages (commands and domain events) to the outbox. Key = order id. */
@Component
public class SagaOutbox {

    static final String AGGREGATE_TYPE = "Order";

    /** Payload of the events published on {@code order.events}. */
    public record OrderEventData(UUID orderId, String customerRef, OrderStatus status, BigDecimal total,
                                 String reason) {
    }

    private final OutboxWriter outbox;

    public SagaOutbox(OutboxWriter outbox) {
        this.outbox = outbox;
    }

    public Mono<Void> command(UUID orderId, Command command) {
        Envelope<Object> envelope = Envelope.of(orderId, orderId, SagaMessages.SAGA_REPLIES, command.data());
        return outbox.write(command.topic(), AGGREGATE_TYPE, orderId.toString(), command.type(), envelope).then();
    }

    public Mono<Void> event(Order order, EventType type, OrderStatus status, String reason) {
        Envelope<OrderEventData> envelope = Envelope.of(order.id(), order.id(), null,
                new OrderEventData(order.id(), order.customerRef(), status, order.total(), reason));
        return outbox.write(SagaMessages.ORDER_EVENTS, AGGREGATE_TYPE, order.id().toString(), type.name(), envelope)
                .then();
    }
}
```

Create `order-service/src/main/java/com/shop/order/service/SagaCoordinator.java`:

```java
package com.shop.order.service;

import java.time.Instant;
import java.util.UUID;

import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.order.domain.OrderExceptions.OrderConflictException;
import com.shop.order.domain.OrderExceptions.OrderNotFoundException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.SagaMachine;
import com.shop.order.domain.SagaMachine.Apply;
import com.shop.order.domain.SagaMachine.Decision;
import com.shop.order.domain.SagaMachine.Ignore;
import com.shop.order.domain.SagaMachine.Refuse;
import com.shop.order.domain.SagaMachine.Transition;
import com.shop.order.domain.SagaMessages.Reply;
import com.shop.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;

/**
 * Imperative shell around the pure {@link SagaMachine}: load the state, ask the machine what to do,
 * apply the answer. Every method assumes it runs inside a transaction opened by its caller (the
 * message dispatcher, the HTTP service, the sweeper), so the state change, both histories and the
 * outbox rows commit together or not at all.
 */
@Service
public class SagaCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SagaCoordinator.class);

    private final OrderRepository repository;
    private final SagaOutbox outbox;
    private final JsonCodec codec;

    public SagaCoordinator(OrderRepository repository, SagaOutbox outbox, JsonCodec codec) {
        this.repository = repository;
        this.outbox = outbox;
        this.codec = codec;
    }

    /** A reply from Product or Inventory. A reply for an unknown order goes to the dead letter topic. */
    public Mono<Void> onReply(UUID orderId, UUID messageId, Reply reply) {
        return repository.load(orderId)
                .switchIfEmpty(Mono.error(() -> new NonRetryableException("Reply for unknown order " + orderId)))
                .flatMap(ctx -> switch (SagaMachine.onReply(ctx, reply)) {
                    case Apply apply -> apply(ctx, apply.transition(), messageId);
                    case Ignore ignore -> {
                        log.info("Order {}: ignored, {}", orderId, ignore.why());
                        yield Mono.<Void>empty();
                    }
                    case Refuse refuse -> Mono.<Void>empty();
                });
    }

    /** Customer cancellation: only a CONFIRMED order. */
    public Mono<Void> cancel(UUID orderId) {
        return repository.load(orderId)
                .switchIfEmpty(Mono.error(() -> new OrderNotFoundException(orderId)))
                .flatMap(ctx -> switch (SagaMachine.cancel(ctx)) {
                    case Apply apply -> apply(ctx, apply.transition(), null);
                    case Refuse refuse -> Mono.<Void>error(new OrderConflictException(refuse.why()));
                    case Ignore ignore -> Mono.<Void>empty();
                });
    }

    /**
     * Timeout of one saga. The staleness is checked again here, on the freshly loaded state, because
     * the saga may have progressed since the sweeper listed it.
     *
     * @return true if a transition was applied
     */
    public Mono<Boolean> timeout(UUID orderId, Instant staleBefore) {
        return repository.load(orderId)
                .filter(ctx -> ctx.saga().updatedAt().isBefore(staleBefore))
                .flatMap(ctx -> switch (SagaMachine.timeout(ctx)) {
                    case Apply apply -> apply(ctx, apply.transition(), null).thenReturn(true);
                    case Ignore ignore -> Mono.just(false);
                    case Refuse refuse -> Mono.just(false);
                })
                .defaultIfEmpty(false);
    }

    private Mono<Void> apply(Loaded ctx, Transition t, UUID messageId) {
        UUID orderId = ctx.order().id();
        OrderStatus current = ctx.order().status();

        Mono<Void> saga = repository.updateSaga(ctx.saga(), t.state(), t.currentStep());
        Mono<Void> orderStatus = t.orderStatus()
                .map(next -> repository.updateOrderStatus(ctx.order(), next)
                        .then(repository.insertStatusHistory(orderId, current, next, t.reason(), messageId)))
                .orElseGet(Mono::empty);
        Mono<Void> pricing = t.pricing()
                .map(p -> repository.applyPricing(ctx.order(), p.unitPrices(), p.total()))
                .orElseGet(Mono::empty);
        Mono<Void> steps = Flux.fromIterable(t.steps())
                .concatMap(entry -> repository.insertStepHistory(orderId, entry, messageId, json(entry.detail())))
                .then();
        Mono<Void> command = t.command().map(c -> outbox.command(orderId, c)).orElseGet(Mono::empty);
        Mono<Void> event = t.event()
                .map(type -> outbox.event(
                        withTotal(ctx, t), type, t.orderStatus().orElse(current), t.reason()))
                .orElseGet(Mono::empty);

        return saga.then(orderStatus).then(pricing).then(steps).then(command).then(event);
    }

    private static com.shop.order.domain.Records.Order withTotal(Loaded ctx, Transition t) {
        var order = ctx.order();
        return t.pricing().isPresent()
                ? new com.shop.order.domain.Records.Order(order.id(), order.customerRef(), order.status(),
                        t.pricing().get().total(), order.idempotencyKey(), order.version())
                : order;
    }

    private String json(Object detail) {
        return detail == null ? null : codec.write(detail);
    }
}
```

- [ ] **Step 4: Create the order service: creation in one transaction with idempotency (lookup first, and a lost race on the unique key falls back to the existing order), reads, history, and cancel with a bounded retry on concurrent updates**

Create `order-service/src/main/java/com/shop/order/service/OrderService.java`:

```java
package com.shop.order.service;

import java.util.List;
import java.util.UUID;

import com.shop.order.domain.OrderExceptions.ConcurrentUpdateException;
import com.shop.order.domain.OrderExceptions.OrderNotFoundException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.SagaMachine.Direction;
import com.shop.order.domain.SagaMachine.StepEntry;
import com.shop.order.domain.SagaMessages;
import com.shop.order.domain.SagaMessages.EventType;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.domain.SagaMessages.ValidateProducts;
import com.shop.order.domain.SagaState;
import com.shop.order.repository.OrderRepository;
import com.shop.order.repository.OrderRepository.StatusHistoryRow;
import com.shop.order.repository.OrderRepository.StepHistoryRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Use cases behind the HTTP API. */
@Service
public class OrderService {

    public record Created(UUID orderId, OrderStatus status) {
    }

    public record History(List<StatusHistoryRow> status, List<StepHistoryRow> steps) {
    }

    private final OrderRepository repository;
    private final SagaOutbox outbox;
    private final SagaCoordinator coordinator;
    private final TransactionalOperator transactions;

    public OrderService(OrderRepository repository, SagaOutbox outbox, SagaCoordinator coordinator,
                        TransactionalOperator transactions) {
        this.repository = repository;
        this.outbox = outbox;
        this.coordinator = coordinator;
        this.transactions = transactions;
    }

    /**
     * Creates the order and starts the saga in ONE transaction: order, items, saga state, both
     * histories, the ValidateProducts command and the OrderCreated event. The same idempotency key
     * always returns the same order, also when two identical requests race.
     */
    public Mono<Created> create(String idempotencyKey, String customerRef, List<Line> items) {
        return existing(idempotencyKey)
                .switchIfEmpty(Mono.defer(() -> transactions.transactional(start(idempotencyKey, customerRef, items))
                        .onErrorResume(DuplicateKeyException.class, e -> existing(idempotencyKey))));
    }

    private Mono<Created> existing(String idempotencyKey) {
        return repository.findOrderByIdempotencyKey(idempotencyKey).map(o -> new Created(o.id(), o.status()));
    }

    private Mono<Created> start(String idempotencyKey, String customerRef, List<Line> items) {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId, customerRef, OrderStatus.PENDING, null, idempotencyKey, 0);
        return repository.insertOrder(orderId, customerRef, idempotencyKey)
                .thenMany(Flux.fromIterable(items).concatMap(i -> repository.insertItem(orderId, i.productId(), i.quantity())))
                .then(repository.insertSaga(orderId, SagaState.STARTED, SagaMessages.STEP_VALIDATE_PRODUCTS))
                .then(repository.insertStatusHistory(orderId, null, OrderStatus.PENDING, "ORDER_CREATED", null))
                .then(repository.insertStepHistory(orderId,
                        new StepEntry(SagaMessages.STEP_VALIDATE_PRODUCTS, Direction.FORWARD, "REQUESTED", null), null, null))
                .then(outbox.command(orderId, new ValidateProducts(items)))
                .then(outbox.event(order, EventType.OrderCreated, OrderStatus.PENDING, "ORDER_CREATED"))
                .thenReturn(new Created(orderId, OrderStatus.PENDING));
    }

    public Mono<Loaded> get(UUID orderId) {
        return repository.load(orderId).switchIfEmpty(Mono.error(() -> new OrderNotFoundException(orderId)));
    }

    public Mono<History> history(UUID orderId) {
        return repository.findOrder(orderId)
                .switchIfEmpty(Mono.error(() -> new OrderNotFoundException(orderId)))
                .then(repository.statusHistory(orderId).collectList()
                        .zipWith(repository.stepHistory(orderId).collectList())
                        .map(t -> new History(t.getT1(), t.getT2())));
    }

    /**
     * Cancels a CONFIRMED order (starts the compensation). A lost race with a concurrent change is
     * retried a few times on fresh state before the caller gets a conflict.
     */
    public Mono<Created> cancel(UUID orderId) {
        return Mono.defer(() -> transactions.transactional(coordinator.cancel(orderId)))
                .retryWhen(Retry.max(3).filter(ConcurrentUpdateException.class::isInstance))
                .then(Mono.defer(() -> repository.findOrder(orderId)))
                .map(o -> new Created(o.id(), o.status()));
    }
}
```

- [ ] **Step 5: Create the functional endpoints and the exception-to-status table**

Create `order-service/src/main/java/com/shop/order/web/InvalidRequestException.java`:

```java
package com.shop.order.web;

import java.util.List;

class InvalidRequestException extends RuntimeException {

    private final List<String> details;

    InvalidRequestException(String message, List<String> details) {
        super(message);
        this.details = List.copyOf(details);
    }

    List<String> details() {
        return details;
    }
}
```

Create `order-service/src/main/java/com/shop/order/web/OrderHandler.java`:

```java
package com.shop.order.web;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.service.OrderService;
import com.shop.order.service.OrderService.Created;
import com.shop.order.service.OrderService.History;
import com.shop.order.web.OrderDtos.AcceptedResponse;
import com.shop.order.web.OrderDtos.CreateOrderRequest;
import com.shop.order.web.OrderDtos.HistoryResponse;
import com.shop.order.web.OrderDtos.ItemResponse;
import com.shop.order.web.OrderDtos.OrderResponse;
import com.shop.order.web.OrderDtos.StatusEntryResponse;
import com.shop.order.web.OrderDtos.StepEntryResponse;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers: each one is {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class OrderHandler {

    static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final OrderService service;
    private final JsonMapper json;

    public OrderHandler(OrderService service, JsonMapper json) {
        this.service = service;
        this.json = json;
    }

    /** 202: the order exists and its saga has started; the final status is reached asynchronously. */
    public Mono<ServerResponse> create(ServerRequest request) {
        String key = request.headers().firstHeader(IDEMPOTENCY_HEADER);
        return request.bodyToMono(CreateOrderRequest.class)
                .switchIfEmpty(Mono.error(() -> new InvalidRequestException("Request body is required", List.of())))
                .flatMap(body -> {
                    List<String> violations = RequestValidation.check(key, body);
                    if (!violations.isEmpty()) {
                        return Mono.error(new InvalidRequestException("Invalid request", violations));
                    }
                    List<Line> items = body.items().stream()
                            .map(i -> new Line(i.productId(), i.quantity())).toList();
                    return service.create(key.trim(), body.customerRef().trim(), items);
                })
                .flatMap(created -> accepted(created));
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(service::get)
                .map(this::toResponse)
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> history(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(id -> service.history(id).map(h -> toResponse(id, h)))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> cancel(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(service::cancel)
                .flatMap(created -> accepted(created));
    }

    private static Mono<ServerResponse> accepted(Created created) {
        return ServerResponse.accepted()
                .location(URI.create("/orders/" + created.orderId()))
                .bodyValue(new AcceptedResponse(created.orderId(), created.status()));
    }

    private OrderResponse toResponse(Loaded loaded) {
        return new OrderResponse(
                loaded.order().id(), loaded.order().customerRef(), loaded.order().status(), loaded.order().total(),
                loaded.items().stream().map(i -> new ItemResponse(i.productId(), i.quantity(), i.unitPrice())).toList(),
                loaded.order().version());
    }

    private HistoryResponse toResponse(UUID orderId, History history) {
        return new HistoryResponse(
                orderId,
                history.status().stream()
                        .map(r -> new StatusEntryResponse(r.from(), r.to(), r.reason(), r.triggerMessageId(), r.occurredAt()))
                        .toList(),
                history.steps().stream()
                        .map(r -> new StepEntryResponse(r.step(), r.direction(), r.outcome(), r.messageId(),
                                r.detail() == null ? null : parse(r.detail()), r.occurredAt()))
                        .toList());
    }

    private JsonNode parse(String text) {
        return json.readTree(text);
    }

    private static UUID orderId(ServerRequest request) {
        String raw = request.pathVariable("id");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid order id", List.of("'" + raw + "' is not a UUID"));
        }
    }
}
```

Create `order-service/src/main/java/com/shop/order/web/OrderRoutes.java`:

```java
package com.shop.order.web;

import java.util.List;

import com.shop.order.domain.OrderExceptions.ConcurrentUpdateException;
import com.shop.order.domain.OrderExceptions.OrderConflictException;
import com.shop.order.domain.OrderExceptions.OrderNotFoundException;
import com.shop.order.web.OrderDtos.ApiError;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/** The HTTP surface as data: a routing table plus an exception-to-status table. */
@Configuration(proxyBeanMethods = false)
public class OrderRoutes {

    @Bean
    public RouterFunction<ServerResponse> orderRouter(OrderHandler handler) {
        return RouterFunctions.route()
                .path("/orders", orders -> orders
                        .POST("", handler::create)
                        .GET("/{id}", handler::get)
                        .GET("/{id}/history", handler::history)
                        .POST("/{id}/cancel", handler::cancel))
                .onError(InvalidRequestException.class, (e, r) -> problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e))
                .onError(OrderNotFoundException.class, (e, r) -> problem(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", e))
                .onError(OrderConflictException.class, (e, r) -> problem(HttpStatus.CONFLICT, "ORDER_CONFLICT", e))
                .onError(ConcurrentUpdateException.class, (e, r) -> problem(HttpStatus.CONFLICT, "CONCURRENT_UPDATE", e))
                .build();
    }

    private static Mono<ServerResponse> problem(HttpStatus status, String code, RuntimeException e) {
        List<String> details = e instanceof InvalidRequestException invalid ? invalid.details() : List.of();
        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ApiError(code, e.getMessage(), details));
    }
}
```

- [ ] **Step 6: Run the API test and see it pass**

```bash
cd order-service && mvn -B verify -Dit.test=OrderApiIT
```

Expected: `Tests run: 6, Failures: 0, Errors: 0` for `OrderApiIT`, `BUILD SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add order-service
git commit -m "feat(order): orders API and saga coordinator"
```

### Task 7: Reacting to the replies of Product and Inventory

**Files:**

- Create: `order-service/src/test/java/com/shop/order/SagaFlowIT.java`
- Create: `order-service/src/main/java/com/shop/order/messaging/{ReplyMessages,SagaReplyHandlers,OrderMessagingConfig,SagaRepliesListener}.java`

- [ ] **Step 1: Write the failing test. The test plays Product and Inventory: it publishes their replies (headers `id`, `type`, like Debezium) and checks the order, both histories and the outbox. Scenarios: nominal flow (total 27.00 from 2 x 10.50 + 2 x 3.00, `OrderConfirmed`); rejected product (no `ReserveStock`, no `ReleaseStock`); insufficient stock (no compensation); cancel of a confirmed order (`CANCELLING`, `ReleaseStock`, then `CANCELLED`, and a second cancel is a `409`); a redelivered and a duplicated reply have no second effect (one `ReserveStock`, one `OrderConfirmed`); a reply for an unknown order is dead-lettered without blocking the next one**

Create `order-service/src/test/java/com/shop/order/SagaFlowIT.java`:

```java
package com.shop.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.shop.order.support.KafkaSupport;
import com.shop.order.support.KafkaTestClient;
import com.shop.order.support.PostgresSupport;
import com.shop.order.support.SagaTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The orchestrator against real PostgreSQL and Kafka, without Debezium and without the other services:
 * the test plays Product and Inventory by publishing their replies (headers {@code id} and {@code type},
 * like Debezium) and checks what the saga did: order status, histories, and the commands and events it
 * wrote to the outbox.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// closes the context (consumer, sweeper) when the class ends, so it cannot touch the database of the next class
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWebTestClient(timeout = "30s")
class SagaFlowIT extends SagaTestBase {

    private static final KafkaTestClient KAFKA = new KafkaTestClient(KafkaSupport.bootstrapServers());

    @BeforeAll
    static void createTopics() throws Exception {
        KAFKA.createTopics(REPLIES, REPLIES + ".DLT");
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        registry.add("shop.kafka.bootstrap-servers", KafkaSupport::bootstrapServers);
        registry.add("shop.kafka.group-id", () -> "order-it-" + UUID.randomUUID());
        registry.add("shop.kafka.concurrency", () -> "1");
        registry.add("shop.kafka.retry.initial-interval", () -> "50ms");
        registry.add("shop.kafka.retry.max-interval", () -> "100ms");
        registry.add("shop.kafka.retry.max-attempts", () -> "2");
        registry.add("shop.order.sweeper-enabled", () -> "false");
    }

    private final UUID a = UUID.randomUUID();
    private final UUID b = UUID.randomUUID();

    private void send(UUID orderId, UUID eventId, String type, String payload) throws Exception {
        KAFKA.send(REPLIES, orderId.toString(), eventId, type, payload);
    }

    private void send(UUID orderId, String type, String payload) throws Exception {
        send(orderId, UUID.randomUUID(), type, payload);
    }

    private Map<UUID, String> prices() {
        Map<UUID, String> prices = new LinkedHashMap<>();
        prices.put(a, "10.50");
        prices.put(b, "3.00");
        return prices;
    }

    /** Creates an order and plays Product and Inventory until it is CONFIRMED. */
    private UUID confirmedOrder() throws Exception {
        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");
        send(orderId, "ProductsValidated", productsValidated(orderId, prices()));
        awaitOutbox(orderId, "ReserveStock");
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));
        awaitStatus(orderId, "CONFIRMED");
        return orderId;
    }

    @Test
    void nominalFlowConfirmsThePricedOrder() throws Exception {
        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");

        send(orderId, "ProductsValidated", productsValidated(orderId, prices()));

        // Inventory is asked to reserve exactly what was ordered
        Map<String, Object> reserve = awaitOutbox(orderId, "ReserveStock");
        assertThat(reserve.get("topic")).isEqualTo("inventory.commands");
        assertThat(payload(reserve).get("data").get("items")).hasSize(2);
        assertThat(statusOf(orderId)).isEqualTo("PENDING");

        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));
        awaitStatus(orderId, "CONFIRMED");

        JsonNode order = order(orderId);
        assertThat(order.get("total").decimalValue()).isEqualByComparingTo("27.00"); // 2 x 10.50 + 2 x 3.00
        assertThat(order.get("items").get(0).get("unitPrice").isNull()).isFalse();

        Map<String, Object> confirmed = awaitOutbox(orderId, "OrderConfirmed");
        assertThat(confirmed.get("topic")).isEqualTo("order.events");
        assertThat(payload(confirmed).get("data").get("status").asString()).isEqualTo("CONFIRMED");
        assertThat(payload(confirmed).get("data").get("total").decimalValue()).isEqualByComparingTo("27.00");

        JsonNode history = history(orderId);
        assertThat(history.get("status")).extracting(n -> n.get("to").asString()).containsExactly("PENDING", "CONFIRMED");
        assertThat(history.get("status").get(1).get("triggerMessageId").isNull()).isFalse();
        assertThat(history.get("steps")).extracting(n -> n.get("step").asString() + ":" + n.get("outcome").asString())
                .containsExactly("ValidateProducts:REQUESTED", "ValidateProducts:VALIDATED", "ReserveStock:RESERVED");
    }

    @Test
    void aRejectedProductRejectsTheOrderAndNothingIsReserved() throws Exception {
        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");

        send(orderId, "ProductsRejected", reply(orderId,
                "{\"rejections\":[{\"productId\":\"%s\",\"reason\":\"NOT_FOUND\"}]}".formatted(b)));

        awaitStatus(orderId, "REJECTED");
        assertThat(awaitOutbox(orderId, "OrderRejected").get("topic")).isEqualTo("order.events");
        assertThat(outbox(orderId, "ReserveStock")).isEmpty();
        assertThat(outbox(orderId, "ReleaseStock")).isEmpty();
        JsonNode steps = history(orderId).get("steps");
        assertThat(steps.get(1).get("outcome").asString()).isEqualTo("REJECTED");
        assertThat(steps.get(1).get("detail").get(0).asString()).isEqualTo(b + ":NOT_FOUND");
    }

    @Test
    void insufficientStockRejectsTheOrderWithoutCompensation() throws Exception {
        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");
        send(orderId, "ProductsValidated", productsValidated(orderId, prices()));
        awaitOutbox(orderId, "ReserveStock");

        send(orderId, "StockRejected", reply(orderId,
                "{\"shortages\":[{\"productId\":\"%s\",\"requested\":2,\"missing\":1}]}".formatted(a)));

        awaitStatus(orderId, "REJECTED");
        awaitOutbox(orderId, "OrderRejected");
        assertThat(outbox(orderId, "ReleaseStock")).isEmpty();
        assertThat(history(orderId).get("status")).extracting(n -> n.get("reason").asString())
                .containsExactly("ORDER_CREATED", "STOCK_REJECTED");
    }

    @Test
    void cancellingAConfirmedOrderReleasesTheStockThenCancels() throws Exception {
        UUID orderId = confirmedOrder();

        assertThat(cancel(orderId)).isEqualTo(HttpStatus.ACCEPTED);

        assertThat(statusOf(orderId)).isEqualTo("CANCELLING");
        Map<String, Object> release = awaitOutbox(orderId, "ReleaseStock");
        assertThat(release.get("topic")).isEqualTo("inventory.commands");
        assertThat(payload(release).get("data").get("reason").asString()).isEqualTo("ORDER_CANCELLED");
        assertThat(cancel(orderId)).as("a second cancel while CANCELLING").isEqualTo(HttpStatus.CONFLICT);

        send(orderId, "StockReleased", reply(orderId, "{\"released\":[]}"));
        awaitStatus(orderId, "CANCELLED");

        awaitOutbox(orderId, "OrderCancelled");
        assertThat(history(orderId).get("steps"))
                .extracting(n -> n.get("direction").asString() + ":" + n.get("step").asString() + ":" + n.get("outcome").asString())
                .containsSubsequence("COMPENSATE:ReleaseStock:REQUESTED", "COMPENSATE:ReleaseStock:RELEASED");
        assertThat(history(orderId).get("status")).extracting(n -> n.get("to").asString())
                .containsExactly("PENDING", "CONFIRMED", "CANCELLING", "CANCELLED");
    }

    @Test
    void aRedeliveredOrDuplicatedReplyHasNoSecondEffect() throws Exception {
        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");
        UUID sameEvent = UUID.randomUUID();
        String validated = productsValidated(orderId, prices());

        send(orderId, sameEvent, "ProductsValidated", validated);               // original
        send(orderId, sameEvent, "ProductsValidated", validated);               // Kafka redelivery: inbox
        send(orderId, UUID.randomUUID(), "ProductsValidated", validated);       // new message, wrong state: ignored
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}")); // late duplicate: ignored

        awaitStatus(orderId, "CONFIRMED"); // same key = same partition: the duplicates were consumed before this
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));

        assertThat(outbox(orderId, "ReserveStock")).hasSize(1);
        assertThat(outbox(orderId, "OrderConfirmed")).hasSize(1);
        assertThat(history(orderId).get("status")).hasSize(2);
    }

    @Test
    void aReplyForAnUnknownOrderGoesToTheDeadLetterTopicAndDoesNotBlockTheNext() throws Exception {
        UUID unknown = UUID.randomUUID();
        send(unknown, "StockReserved", reply(unknown, "{\"reservations\":[]}"));

        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");
        send(orderId, "ProductsValidated", productsValidated(orderId, prices()));
        awaitOutbox(orderId, "ReserveStock");

        assertThat(KAFKA.readUntil(REPLIES + ".DLT", r -> unknown.toString().equals(r.key()), Duration.ofSeconds(30)))
                .isPresent();
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd order-service && mvn -B verify -Dit.test=SagaFlowIT
```

Expected: `ConditionTimeout` (nothing consumes `order.saga.replies` yet).

- [ ] **Step 3: Create the reply contract and the handlers: one small function per reply type that parses the envelope, builds the domain `Reply`, and hands it to the coordinator. Unusable payloads are `NonRetryableException`s**

Create `order-service/src/main/java/com/shop/order/messaging/ReplyMessages.java`:

```java
package com.shop.order.messaging;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Payloads (the {@code data} of the envelope) of the replies on {@code order.saga.replies}. */
public final class ReplyMessages {

    public static final String TOPIC = "order.saga.replies";
    public static final String PRODUCTS_VALIDATED = "ProductsValidated";
    public static final String PRODUCTS_REJECTED = "ProductsRejected";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";
    public static final String STOCK_RELEASED = "StockReleased";

    private ReplyMessages() {
    }

    public record ProductsValidatedData(List<Priced> items) {

        public record Priced(UUID productId, BigDecimal unitPrice) {
        }
    }

    public record ProductsRejectedData(List<Rejection> rejections) {

        public record Rejection(UUID productId, String reason) {
        }
    }

    public record StockRejectedData(List<Shortage> shortages) {

        public record Shortage(UUID productId, long requested, long missing) {
        }
    }
}
```

Create `order-service/src/main/java/com/shop/order/messaging/SagaReplyHandlers.java`:

```java
package com.shop.order.messaging;

import java.util.function.Function;
import java.util.stream.Collectors;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.order.domain.SagaMessages.ProductsRejected;
import com.shop.order.domain.SagaMessages.ProductsValidated;
import com.shop.order.domain.SagaMessages.Reply;
import com.shop.order.domain.SagaMessages.StockRejected;
import com.shop.order.domain.SagaMessages.StockReleased;
import com.shop.order.domain.SagaMessages.StockReserved;
import com.shop.order.messaging.ReplyMessages.ProductsRejectedData;
import com.shop.order.messaging.ReplyMessages.ProductsValidatedData;
import com.shop.order.messaging.ReplyMessages.StockRejectedData;
import com.shop.order.service.SagaCoordinator;
import tools.jackson.databind.JsonNode;

/**
 * One tiny function per reply type: parse the envelope, turn the payload into a domain {@link Reply},
 * hand it to the coordinator. Parsing problems are not retryable (dead letter topic).
 */
public final class SagaReplyHandlers {

    private SagaReplyHandlers() {
    }

    public static HandlerRegistry registry(SagaCoordinator coordinator, JsonCodec codec) {
        return HandlerRegistry.builder()
                .on(ReplyMessages.PRODUCTS_VALIDATED, handler(coordinator, codec, ProductsValidatedData.class,
                        data -> new ProductsValidated(data.items().stream().collect(Collectors.toMap(
                                ProductsValidatedData.Priced::productId, ProductsValidatedData.Priced::unitPrice)))))
                .on(ReplyMessages.PRODUCTS_REJECTED, handler(coordinator, codec, ProductsRejectedData.class,
                        data -> new ProductsRejected(data.rejections().stream()
                                .map(r -> r.productId() + ":" + r.reason()).toList())))
                .on(ReplyMessages.STOCK_RESERVED, handler(coordinator, codec, JsonNode.class,
                        data -> new StockReserved()))
                .on(ReplyMessages.STOCK_REJECTED, handler(coordinator, codec, StockRejectedData.class,
                        data -> new StockRejected(data.shortages().stream()
                                .map(s -> s.productId() + ": requested " + s.requested() + ", missing " + s.missing())
                                .toList())))
                .on(ReplyMessages.STOCK_RELEASED, handler(coordinator, codec, JsonNode.class,
                        data -> new StockReleased()))
                .build();
    }

    private static <D> Handler handler(SagaCoordinator coordinator, JsonCodec codec, Class<D> dataType,
                                       Function<D, Reply> toReply) {
        return message -> {
            Envelope<D> envelope = message.envelope(codec, dataType);
            if (envelope.sagaId() == null) {
                throw new NonRetryableException(message.type() + " without sagaId");
            }
            Reply reply;
            try {
                reply = toReply.apply(envelope.data());
            } catch (NullPointerException | IllegalStateException e) {
                throw new NonRetryableException(message.type() + " has an unusable payload", e);
            }
            return coordinator.onReply(envelope.sagaId(), message.eventId(), reply);
        };
    }
}
```

- [ ] **Step 4: Wire the consumer (registry, dispatcher with consumer name `order-service.order.saga.replies`, single listener)**

Create `order-service/src/main/java/com/shop/order/messaging/OrderMessagingConfig.java`:

```java
package com.shop.order.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.order.service.SagaCoordinator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: the replies of Product and Inventory, handled by the saga coordinator. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class OrderMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "order-service." + ReplyMessages.TOPIC;

    @Bean
    public HandlerRegistry sagaReplyHandlers(SagaCoordinator coordinator, JsonCodec codec) {
        return SagaReplyHandlers.registry(coordinator, codec);
    }

    @Bean
    public MessageDispatcher sagaRepliesDispatcher(
            HandlerRegistry sagaReplyHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(CONSUMER_NAME, sagaReplyHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
```

Create `order-service/src/main/java/com/shop/order/messaging/SagaRepliesListener.java`:

```java
package com.shop.order.messaging;

import com.shop.common.config.KafkaConsumerConfig;
import com.shop.common.messaging.MessageDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** The only Kafka entry point of the service: hands every reply to the dispatcher. */
@Component
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class SagaRepliesListener {

    private final MessageDispatcher dispatcher;

    public SagaRepliesListener(MessageDispatcher sagaRepliesDispatcher) {
        this.dispatcher = sagaRepliesDispatcher;
    }

    @KafkaListener(topics = ReplyMessages.TOPIC, containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY)
    void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        dispatcher.onMessage(record, acknowledgment);
    }
}
```

- [ ] **Step 5: Run the test and see it pass**

```bash
cd order-service && mvn -B verify -Dit.test=SagaFlowIT
```

Expected: `Tests run: 6, Failures: 0, Errors: 0` for `SagaFlowIT`.

- [ ] **Step 6: Commit**

```bash
git add order-service
git commit -m "feat(order): saga reacts to Product and Inventory replies"
```

### Task 8: Timeout sweeper

**Files:**

- Create: `order-service/src/test/java/com/shop/order/SagaTimeoutIT.java`
- Create: `order-service/src/main/java/com/shop/order/service/{OrderProperties,SagaTimeoutSweeper}.java`

- [ ] **Step 1: Write the failing test. Timeout 2 seconds, sweep every 500 ms. Silence from Product rejects the order (reason `TIMEOUT`) and a late `ProductsValidated` is ignored; silence from Inventory compensates (`CANCELLING`, `ReleaseStock` with reason `TIMEOUT`) and the `StockReleased` that follows cancels the order. A `@BeforeEach` first proves the listener is consuming (see decision 7)**

Create `order-service/src/test/java/com/shop/order/SagaTimeoutIT.java`:

```java
package com.shop.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import com.shop.order.support.KafkaSupport;
import com.shop.order.support.KafkaTestClient;
import com.shop.order.support.PostgresSupport;
import com.shop.order.support.SagaTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The sweeper with a 2 second timeout: silence from the other services must never leave an order hanging. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// closes the context (consumer, sweeper) when the class ends, so it cannot touch the database of the next class
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWebTestClient(timeout = "30s")
class SagaTimeoutIT extends SagaTestBase {

    private static final KafkaTestClient KAFKA = new KafkaTestClient(KafkaSupport.bootstrapServers());

    @BeforeAll
    static void createTopics() throws Exception {
        KAFKA.createTopics(REPLIES, REPLIES + ".DLT");
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        registry.add("shop.kafka.bootstrap-servers", KafkaSupport::bootstrapServers);
        registry.add("shop.kafka.group-id", () -> "order-timeout-it-" + UUID.randomUUID());
        registry.add("shop.kafka.concurrency", () -> "1");
        registry.add("shop.order.saga-timeout", () -> "2s");
        registry.add("shop.order.sweep-interval", () -> "500ms");
    }

    private final UUID a = UUID.randomUUID();

    /**
     * The first rebalance of a new consumer group can take longer than the 2 second timeout under test.
     * A reply for an unknown order is dead-lettered at once, so seeing it in the DLT proves the listener
     * is assigned and working.
     */
    @BeforeEach
    void waitUntilTheListenerIsConsuming() throws Exception {
        UUID probe = UUID.randomUUID();
        send(probe, "StockReserved", reply(probe, "{\"reservations\":[]}"));
        assertThat(KAFKA.readUntil(REPLIES + ".DLT", r -> probe.toString().equals(r.key()), Duration.ofSeconds(60)))
                .isPresent();
    }

    private void send(UUID orderId, String type, String payload) throws Exception {
        KAFKA.send(REPLIES, orderId.toString(), UUID.randomUUID(), type, payload);
    }

    @Test
    void noAnswerFromProductRejectsTheOrderAndALateAnswerIsIgnored() throws Exception {
        UUID orderId = createOrder(a);

        awaitStatus(orderId, "REJECTED");

        awaitOutbox(orderId, "OrderRejected");
        assertThat(history(orderId).get("status").get(1).get("reason").asString()).isEqualTo("TIMEOUT");
        assertThat(history(orderId).get("steps").get(1).get("outcome").asString()).isEqualTo("TIMED_OUT");

        // Product answers after the timeout: the saga is already REJECTED, nothing must happen
        send(orderId, "ProductsValidated", productsValidated(orderId, Map.of(a, "5.00")));
        UUID marker = createOrder(UUID.randomUUID());
        awaitOutbox(marker, "ValidateProducts");
        assertThat(statusOf(orderId)).isEqualTo("REJECTED");
        assertThat(outbox(orderId, "ReserveStock")).isEmpty();
    }

    @Test
    void noAnswerFromInventoryCompensatesAndTheReleaseCancelsTheOrder() throws Exception {
        UUID orderId = createOrder(a);
        awaitOutbox(orderId, "ValidateProducts");
        send(orderId, "ProductsValidated", productsValidated(orderId, Map.of(a, "5.00")));
        awaitOutbox(orderId, "ReserveStock");

        // Inventory stays silent: the sweeper asks it to release whatever it may have reserved
        awaitStatus(orderId, "CANCELLING");
        Map<String, Object> release = awaitOutbox(orderId, "ReleaseStock");
        assertThat(payload(release).get("data").get("reason").asString()).isEqualTo("TIMEOUT");

        send(orderId, "StockReleased", reply(orderId, "{\"released\":[]}"));
        awaitStatus(orderId, "CANCELLED");
        awaitOutbox(orderId, "OrderCancelled");
        assertThat(history(orderId).get("status")).extracting(n -> n.get("reason").asString())
                .containsSubsequence("ORDER_CREATED", "TIMEOUT", "STOCK_RELEASED");
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd order-service && mvn -B verify -Dit.test=SagaTimeoutIT
```

Expected: `ConditionTimeout` (orders stay `PENDING`).

- [ ] **Step 3: Create the properties and the sweeper. The cutoff is computed inside `Mono.defer`, because Spring re-subscribes to the returned `Mono` on every run (decision 6); each saga runs in its own transaction and a failure on one saga does not stop the sweep**

Create `order-service/src/main/java/com/shop/order/service/OrderProperties.java`:

```java
package com.shop.order.service;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** {@code shop.order.*}: how long a saga may stay without progress, and how the sweeper polls. */
@ConfigurationProperties(prefix = "shop.order")
public record OrderProperties(
        @DefaultValue("2m") Duration sagaTimeout,
        @DefaultValue("30s") Duration sweepInterval,
        @DefaultValue("100") int sweepBatchSize,
        @DefaultValue("true") boolean sweeperEnabled) {
}
```

Create `order-service/src/main/java/com/shop/order/service/SagaTimeoutSweeper.java`:

```java
package com.shop.order.service;

import java.time.Instant;

import com.shop.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Safety net of the saga: a saga that made no progress for {@code shop.order.saga-timeout} is rejected
 * (nothing reserved yet) or compensated (a reservation may exist). Each saga is handled in its own
 * transaction; a saga that moved in the meantime is skipped, and a lost optimistic race is simply left
 * for the next sweep. Safe to run on several instances.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OrderProperties.class)
@ConditionalOnProperty(prefix = "shop.order", name = "sweeper-enabled", havingValue = "true", matchIfMissing = true)
public class SagaTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

    private final OrderRepository repository;
    private final SagaCoordinator coordinator;
    private final TransactionalOperator transactions;
    private final OrderProperties properties;

    public SagaTimeoutSweeper(OrderRepository repository, SagaCoordinator coordinator,
                              TransactionalOperator transactions, OrderProperties properties) {
        this.repository = repository;
        this.coordinator = coordinator;
        this.transactions = transactions;
        this.properties = properties;
    }

    /**
     * Reactive {@code @Scheduled} method. Spring calls it once and re-subscribes to the returned Mono on
     * every run, so the cutoff is computed at subscription ({@code Mono.defer}), not at call time.
     */
    @Scheduled(fixedDelayString = "${shop.order.sweep-interval:30s}",
            initialDelayString = "${shop.order.sweep-interval:30s}")
    public Mono<Void> sweep() {
        return Mono.defer(() -> {
            Instant staleBefore = Instant.now().minus(properties.sagaTimeout());
            return repository.findStaleSagas(staleBefore, properties.sweepBatchSize())
                    .concatMap(id -> transactions.transactional(coordinator.timeout(id, staleBefore))
                            .doOnNext(applied -> {
                                if (applied) {
                                    log.warn("Saga {} timed out", id);
                                }
                            })
                            .onErrorResume(e -> {
                                log.info("Saga {} skipped this sweep: {}", id, e.toString());
                                return Mono.empty();
                            }))
                    .then();
        });
    }
}
```

- [ ] **Step 4: Run the whole build**

```bash
cd order-service && mvn -B verify
```

Expected: 20 unit tests, then `OrderApiIT` 6, `SagaFlowIT` 6, `SagaTimeoutIT` 2: `Tests run: 14, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add order-service
git commit -m "feat(order): saga timeout sweeper"
```

### Task 9: Orchestration through Debezium (manual check)

**Files:**

- Modify: none

- [ ] **Step 1: Start the infrastructure (from plan 1)**

```bash
cd infra
docker compose up -d --wait order-db kafka connect
docker compose up kafka-init
```

Expected: services healthy, `kafka-init` exits 0.

- [ ] **Step 2: Start the service in a second terminal. Flyway creates the order tables and the outbox/inbox tables in `order_db` (host port 5442)**

```bash
cd order-service && mvn -B spring-boot:run
```

Expected log lines: Flyway `Successfully applied 2 migrations`, then `Started OrderServiceApplication`.

- [ ] **Step 3: Register the order connector (the outbox table now exists)**

```bash
cd infra && ./scripts/register-connectors.sh order
```

Expected: the final JSON lists `order-outbox` with state `RUNNING`.

- [ ] **Step 4: Create an order**

```bash
PRODUCT_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
curl -s -i -X POST localhost:8080/orders -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d "{\"customerRef\":\"c1\",\"items\":[{\"productId\":\"$PRODUCT_ID\",\"quantity\":2}]}"
```

Expected: `202`, a `Location` header and `{"orderId":...,"status":"PENDING"}`. Keep `ORDER_ID` (the `orderId`) in your shell. Repeating the same command returns the same order.

- [ ] **Step 5: Check that Debezium published the command**

```bash
cd infra && docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic product.commands --from-beginning --max-messages 1 \
  --timeout-ms 30000 --property print.headers=true --property print.key=true
```

Expected: header `type:ValidateProducts`, key = the order id, `replyTo` = `order.saga.replies`.

- [ ] **Step 6: Play Product and Inventory. The other services are not part of this check, so insert their replies into the order database's own outbox: Debezium publishes them to `order.saga.replies` and the service consumes them. First `ProductsValidated`**

```bash
cd infra && docker compose exec -T order-db psql -U postgres -d order_db <<SQL
INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
VALUES (gen_random_uuid(), 'order.saga.replies', 'Order', '$ORDER_ID', 'ProductsValidated',
        '{"sagaId":"$ORDER_ID","correlationId":"$ORDER_ID","replyTo":"order.saga.replies","data":{"items":[{"productId":"$PRODUCT_ID","unitPrice":12.50}]}}');
SQL
curl -s localhost:8080/orders/$ORDER_ID
```

Expected: the order shows `"total":25.00` and is still `PENDING`; `inventory.commands` now holds a `ReserveStock` for it (read it like `product.commands` above).

- [ ] **Step 7: Then `StockReserved`**

```bash
cd infra && docker compose exec -T order-db psql -U postgres -d order_db <<SQL
INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
VALUES (gen_random_uuid(), 'order.saga.replies', 'Order', '$ORDER_ID', 'StockReserved',
        '{"sagaId":"$ORDER_ID","correlationId":"$ORDER_ID","replyTo":"order.saga.replies","data":{"reservations":[]}}');
SQL
sleep 3; curl -s localhost:8080/orders/$ORDER_ID; curl -s localhost:8080/orders/$ORDER_ID/history
```

Expected: status `CONFIRMED`; the history lists `PENDING`, `CONFIRMED` and the steps `ValidateProducts REQUESTED`, `ValidateProducts VALIDATED`, `ReserveStock RESERVED`; `order.events` holds `OrderCreated` and `OrderConfirmed`.

- [ ] **Step 8: Cancel it**

```bash
curl -s -i -X POST localhost:8080/orders/$ORDER_ID/cancel
```

Expected: `202` with status `CANCELLING`, and a `ReleaseStock` on `inventory.commands`. Insert a `StockReleased` reply the same way to see `CANCELLED` and the `OrderCancelled` event. If nothing arrives at some step, check in this order: the service log (handler error or DLT), `curl -s localhost:8083/connectors/order-outbox/status`, and `order.saga.replies.DLT`. This is the first time the loop runs through Debezium.

- [ ] **Step 9: Stop the service (Ctrl+C) and the stack**

```bash
cd infra && docker compose down
```

---

## Done when

- `cd order-service && mvn -B verify` is green (20 unit + 14 integration tests).
- The Debezium check of Task 9 takes an order to `CONFIRMED` and then `CANCELLING`.

Then continue with plan 5: end-to-end tests with Product, Inventory and Order running together through Debezium (nominal flow, rejected product, rejected stock, cancellation with compensation, redelivery, concurrent stock on a multi-bucket product, and the timeout).
