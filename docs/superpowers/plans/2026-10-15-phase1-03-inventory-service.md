# Phase 1, plan 3: Inventory service

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `inventory-service`: bucketed stock with an admin API, and the saga steps `ReserveStock` (reserve or reject, never leaving a phantom reservation) and `ReleaseStock` (compensation).

**Architecture:** Same shape as `product-service` (plan 2), on `shop-common`. Decisions are pure functions (`BucketScan`, `StockShares`, `StockNeeds`, `Allocation.decide`); effects are single atomic SQL statements. Reservation never reads then writes: one conditional `UPDATE ... RETURNING` per bucket, no `FOR UPDATE`, so no pooled connection is held waiting on a lock and there is no retry storm. Hot products are sharded into buckets.

**Tech Stack:** Java 21, Spring Boot 4.1.1 (WebFlux), R2DBC `DatabaseClient`, Flyway, PostgreSQL 18, Kafka via Spring Kafka (inside `shop-common`), Testcontainers 2.0.5.

**Spec:** `docs/superpowers/specs/2026-10-15-ecommerce-phase1-design.md` (sections 5, 6, 7, 9, 10; section 7 was amended while writing this plan, see "Decisions" below)

**Prerequisite:** plans 1 and 2 done (`shop-common` installed in `~/.m2`). Work on a branch, for example `feat/inventory-service`.

## Verification status (read before starting)

The code in this plan was compiled and tested before being written here, in a scratch copy:

- 17 unit tests: green.
- 18 integration tests (`StockApiIT` 7, `ConcurrentReservationIT` 3, `StockCommandsIT` 8): green, four runs in a row, against **PostgreSQL 16** and an **embedded Kafka (KRaft) broker**, because the authoring sandbox had no Docker daemon.

**Not executed yet, so Tasks 5 to 8 (Testcontainers) and Task 9 (Debezium) are the first real run of:** the Testcontainers support classes, PostgreSQL 18, and the loop through Debezium.

## Decisions (and why the spec changed)

1. **Reservation undo in the same transaction, not a second transaction.** The spec said: roll back, then write `StockRejected` in a second transaction. That needs `REQUIRES_NEW`, i.e. two pooled connections per message. Instead, reservation rows and movements are written only after every product is covered; on a shortage the units already taken are given back with `UPDATE ... SET reserved = reserved - qty`, and the message transaction commits only the inbox row and the `StockRejected` outbox row. Same observable result (no phantom reservation), one connection.
2. **`last_taken` column.** `UPDATE ... RETURNING` cannot see the old row (PostgreSQL 18 adds `OLD`, earlier versions do not). A scratch column set by the same statement tells the caller how much that statement took, with no read-modify-write.
3. **Scan order and deadlocks.** Products are locked in sorted order. Within a product the scan starts at `hash(orderId) mod N`, goes up, then wraps to 0, so two orders that both overflow into the next bucket could lock in opposite orders. That needs a nearly sold-out product; PostgreSQL aborts one transaction, and the Kafka consumer replays the message (`DeadlockLoserDataAccessException` is not a `NonRetryableException`). `ConcurrentReservationIT` replays the same way a consumer does. The 4 runs saw no deadlock; the retry is the safety net, not an expected path.
4. **Flyway versions:** `shop-common` owns `V1`; this service starts at `V2`.
5. **A handler that throws synchronously** is not reached by `RouterFunctions.route().onError(...)`: request checks run inside `Mono.fromSupplier` (learned in plan 2).

## File structure

```
inventory-service/
  pom.xml
  src/main/resources/application.yml
  src/main/resources/db/migration/V2__inventory.sql
  src/main/java/com/shop/inventory/
    InventoryServiceApplication
    domain/      BucketScan, StockShares, StockNeeds, Allocation   (pure)
                 StockNotFoundException, StockConflictException
    repository/  StockRepository                                    <- all SQL
    service/     ReservationService (reserve/release), StockAdminService
    web/         StockDtos, RequestValidation (pure), InvalidRequestException,
                 StockHandler, StockRoutes
    messaging/   InventoryMessages, ReserveStockHandler, ReleaseStockHandler,
                 InventoryMessagingConfig, InventoryCommandsListener
  src/test/java/com/shop/inventory/
    domain/{BucketScanTest,StockSharesTest,StockNeedsTest,AllocationTest}, web/RequestValidationTest   (unit)
    StockApiIT, ConcurrentReservationIT, StockCommandsIT                                               (Docker)
    support/PostgresSupport, KafkaSupport, KafkaTestClient
```

Conventions: unit tests are `*Test` (Surefire, no Docker); integration tests are `*IT` (Failsafe, Docker needed). Commands run from the `ecommerce/` directory unless stated.

Message contract implemented here (the Order plan produces and consumes the same shapes). The order id is the saga id (`saga_id = order_id`) and the Kafka key.

| Direction | Topic | Type | `data` |
|---|---|---|---|
| in | `inventory.commands` | `ReserveStock` | `{"items":[{"productId":"<uuid>","quantity":3}]}` (duplicates summed, quantity must be > 0) |
| in | `inventory.commands` | `ReleaseStock` | `{"reason":"ORDER_CANCELLED"}` (reason optional) |
| out | `replyTo` of the command | `StockReserved` | `{"reservations":[{"productId":"<uuid>","bucketNo":2,"quantity":3}]}` |
| out | `replyTo` of the command | `StockRejected` | `{"shortages":[{"productId":"<uuid>","requested":9,"missing":4}]}` |
| out | `replyTo` of the command | `StockReleased` | `{"released":[{"productId":"<uuid>","bucketNo":2,"quantity":3}]}` (empty list when nothing was reserved) |

Malformed commands (unparseable JSON, no `sagaId`, no `replyTo`, no `items`, quantity <= 0) are `NonRetryableException`s: straight to `inventory.commands.DLT`.

Admin API: `PUT /stock/{productId}` with `{"quantity":100,"bucketCount":8}` sets the on-hand quantity (free stock becomes `quantity - reserved`, split evenly over the buckets); `409 STOCK_CONFLICT` if the quantity is below what is reserved or a bucket that still holds reservations would be removed. `GET /stock/{productId}` returns totals and per-bucket figures (`404` if never set).

---

### Task 1: Scaffold the service

**Files:**

- Create: `inventory-service/pom.xml`
- Create: `inventory-service/src/main/resources/application.yml`
- Create: `inventory-service/src/main/resources/db/migration/V2__inventory.sql`
- Create: `inventory-service/src/main/java/com/shop/inventory/InventoryServiceApplication.java`

- [ ] **Step 1: Create the POM (same dependencies as `product-service`)**

Create `inventory-service/pom.xml`:

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
    <artifactId>inventory-service</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>inventory-service</name>
    <description>Stock levels (bucketed) and the stock reservation steps of the order saga.</description>

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

- [ ] **Step 2: Create the configuration (port 8082, database `inventory_db` on host port 5443)**

Create `inventory-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: inventory-service

server:
  port: 8082

shop:
  r2dbc:
    host: ${DB_HOST:localhost}
    port: ${DB_PORT:5443}
    database: ${DB_NAME:inventory_db}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:postgres}
    postgres:
      application-name: inventory-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:29092}
    group-id: inventory-service

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

- [ ] **Step 3: Create the schema. Four tables: bucket configuration, buckets, reservations, append-only movements**

Create `inventory-service/src/main/resources/db/migration/V2__inventory.sql`:

```sql
-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.

-- How many buckets ("shards") a product's stock is split into. Default 1; hot products get more.
CREATE TABLE product_stock_config (
    product_id   UUID PRIMARY KEY,
    bucket_count INT         NOT NULL DEFAULT 1 CHECK (bucket_count BETWEEN 1 AND 256),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- available = quantity on hand in this bucket, reserved = part of it promised to orders.
-- free stock of a bucket = available - reserved.
CREATE TABLE stock_buckets (
    product_id UUID   NOT NULL,
    bucket_no  INT    NOT NULL CHECK (bucket_no >= 0),
    available  BIGINT NOT NULL CHECK (available >= 0),
    reserved   BIGINT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    -- scratch value: quantity taken by the latest reservation UPDATE. It lets one atomic UPDATE
    -- report what it took (UPDATE ... RETURNING cannot see the old row before PostgreSQL 18's OLD).
    last_taken BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (product_id, bucket_no),
    CONSTRAINT ck_bucket_reserved_within_available CHECK (reserved <= available)
);

CREATE TABLE stock_reservations (
    order_id   UUID        NOT NULL,
    product_id UUID        NOT NULL,
    bucket_no  INT         NOT NULL,
    qty        BIGINT      NOT NULL CHECK (qty > 0),
    status     VARCHAR(16) NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, product_id, bucket_no)
);

-- Append-only history. delta = change of FREE stock: RESERVE < 0, RELEASE > 0, ADJUST either sign.
CREATE TABLE stock_movements (
    id          BIGSERIAL PRIMARY KEY,
    product_id  UUID        NOT NULL,
    order_id    UUID,
    delta       BIGINT      NOT NULL,
    type        VARCHAR(16) NOT NULL CHECK (type IN ('RESERVE', 'RELEASE', 'ADJUST')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_stock_movements_product ON stock_movements (product_id, occurred_at);
CREATE INDEX idx_stock_movements_order ON stock_movements (order_id) WHERE order_id IS NOT NULL;
```

- [ ] **Step 4: Create the application class**

Create `inventory-service/src/main/java/com/shop/inventory/InventoryServiceApplication.java`:

```java
package com.shop.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
```

- [ ] **Step 5: Compile**

```bash
cd inventory-service && mvn -B -q compile
```

Expected: no output, exit code 0.

- [ ] **Step 6: Commit**

```bash
git add inventory-service
git commit -m "build(inventory): scaffold inventory-service"
```

### Task 2: Bucket scan and stock shares (pure)

**Files:**

- Create: `inventory-service/src/test/java/com/shop/inventory/domain/{BucketScanTest,StockSharesTest}.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/domain/{BucketScan,StockShares}.java`

- [ ] **Step 1: Write the failing tests**

Create `inventory-service/src/test/java/com/shop/inventory/domain/BucketScanTest.java`:

```java
package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class BucketScanTest {

    @Test
    void visitsEveryBucketExactlyOnce() {
        UUID orderId = UUID.randomUUID();

        List<Integer> scan = BucketScan.order(orderId, 8);

        assertThat(scan).hasSize(8).doesNotHaveDuplicates().containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void startsAtTheHashBucketGoesUpThenWrapsToZero() {
        UUID orderId = new UUID(0, 5); // hashCode() == 5 for (0, 5)
        assertThat(orderId.hashCode()).isEqualTo(5);

        assertThat(BucketScan.order(orderId, 8)).containsExactly(5, 6, 7, 0, 1, 2, 3, 4);
        assertThat(BucketScan.order(orderId, 4)).containsExactly(1, 2, 3, 0); // 5 mod 4 = 1
    }

    @Test
    void aSingleBucketAlwaysScansBucketZero() {
        assertThat(BucketScan.order(UUID.randomUUID(), 1)).containsExactly(0);
    }

    @Test
    void noBucketsMeansNothingToScan() {
        assertThat(BucketScan.order(UUID.randomUUID(), 0)).isEmpty();
    }

    @Test
    void negativeHashCodesStayInRange() {
        UUID negative = new UUID(0, 0xFFFFFFFFL);
        assertThat(negative.hashCode()).isNegative();
        assertThat(BucketScan.startBucket(negative, 16)).isBetween(0, 15);
    }

    @Test
    void ordersSpreadAcrossAllStartBuckets() {
        Set<Integer> starts = new HashSet<>();
        IntStream.range(0, 2_000).forEach(i -> starts.add(BucketScan.startBucket(UUID.randomUUID(), 16)));

        assertThat(starts).hasSize(16);
    }
}
```

Create `inventory-service/src/test/java/com/shop/inventory/domain/StockSharesTest.java`:

```java
package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class StockSharesTest {

    @Test
    void splitsEvenly() {
        assertThat(StockShares.split(100, 4)).containsExactly(25, 25, 25, 25);
    }

    @Test
    void givesTheRemainderToTheFirstBuckets() {
        long[] shares = StockShares.split(10, 4);

        assertThat(shares).containsExactly(3, 3, 2, 2);
        assertThat(Arrays.stream(shares).sum()).isEqualTo(10);
    }

    @Test
    void handlesFewerUnitsThanBuckets() {
        assertThat(StockShares.split(2, 4)).containsExactly(1, 1, 0, 0);
        assertThat(StockShares.split(0, 3)).containsExactly(0, 0, 0);
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> StockShares.split(-1, 2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StockShares.split(5, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: Run them and see them fail**

```bash
cd inventory-service && mvn -B -q test -Dtest='BucketScanTest,StockSharesTest'
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... BucketScan` / `StockShares`.

- [ ] **Step 3: Implement**

Create `inventory-service/src/main/java/com/shop/inventory/domain/BucketScan.java`:

```java
package com.shop.inventory.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Order in which an order's reservation visits the buckets of a product. Pure.
 *
 * <p>The first bucket is {@code hash(orderId) mod bucketCount}, so concurrent orders spread over
 * the buckets instead of fighting for the same row. The scan then goes up to the last bucket and
 * wraps to 0. Buckets are therefore locked in ascending order except at the wrap point; a deadlock
 * needs two orders to both spill over, which only happens when the product is nearly sold out.
 * PostgreSQL aborts one of them, the whole message transaction rolls back and the Kafka consumer
 * retries it.
 */
public final class BucketScan {

    private BucketScan() {
    }

    public static int startBucket(UUID orderId, int bucketCount) {
        return Math.floorMod(orderId.hashCode(), bucketCount);
    }

    public static List<Integer> order(UUID orderId, int bucketCount) {
        if (bucketCount <= 0) {
            return List.of();
        }
        int start = startBucket(orderId, bucketCount);
        List<Integer> scan = new ArrayList<>(bucketCount);
        for (int b = start; b < bucketCount; b++) {
            scan.add(b);
        }
        for (int b = 0; b < start; b++) {
            scan.add(b);
        }
        return List.copyOf(scan);
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/domain/StockShares.java`:

```java
package com.shop.inventory.domain;

/** Splits a quantity over buckets as evenly as possible; the remainder goes to the first buckets. Pure. */
public final class StockShares {

    private StockShares() {
    }

    public static long[] split(long quantity, int bucketCount) {
        if (quantity < 0 || bucketCount <= 0) {
            throw new IllegalArgumentException("quantity >= 0 and bucketCount > 0 required");
        }
        long[] shares = new long[bucketCount];
        long base = quantity / bucketCount;
        long remainder = quantity % bucketCount;
        for (int i = 0; i < bucketCount; i++) {
            shares[i] = base + (i < remainder ? 1 : 0);
        }
        return shares;
    }
}
```

- [ ] **Step 4: Run them and see them pass**

```bash
cd inventory-service && mvn -B test -Dtest='BucketScanTest,StockSharesTest'
```

Expected: `Tests run: 10, Failures: 0, Errors: 0` (6 + 4).

- [ ] **Step 5: Commit**

```bash
git add inventory-service
git commit -m "feat(inventory): bucket scan order and stock shares"
```

### Task 3: Needs and allocation outcome (pure)

**Files:**

- Create: `inventory-service/src/test/java/com/shop/inventory/domain/{StockNeedsTest,AllocationTest}.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/domain/{StockNeeds,Allocation}.java`

- [ ] **Step 1: Write the failing tests**

Create `inventory-service/src/test/java/com/shop/inventory/domain/StockNeedsTest.java`:

```java
package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.domain.StockNeeds.Line;
import org.junit.jupiter.api.Test;

class StockNeedsTest {

    private static final UUID LOW = new UUID(0, 1);
    private static final UUID HIGH = new UUID(0, 2);

    @Test
    void sumsDuplicatedProducts() {
        var needs = StockNeeds.merge(List.of(new Line(LOW, 2), new Line(LOW, 3)));

        assertThat(needs).containsExactly(java.util.Map.entry(LOW, 5L));
    }

    @Test
    void sortsByProductIdWhateverTheInputOrder() {
        var needs = StockNeeds.merge(List.of(new Line(HIGH, 1), new Line(LOW, 1)));

        assertThat(needs.keySet()).containsExactly(LOW, HIGH);
    }

    @Test
    void rejectsNonPositiveQuantities() {
        assertThatThrownBy(() -> StockNeeds.merge(List.of(new Line(LOW, 0))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StockNeeds.merge(List.of(new Line(LOW, -4))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

Create `inventory-service/src/test/java/com/shop/inventory/domain/AllocationTest.java`:

```java
package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.ProductAllocation;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.domain.Allocation.Shortage;
import com.shop.inventory.domain.Allocation.Take;
import org.junit.jupiter.api.Test;

class AllocationTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    @Test
    void reservesWhenEveryProductIsCovered() {
        var a = new ProductAllocation(A, 5, List.of(new Take(A, 0, 3), new Take(A, 1, 2)), 0);
        var b = new ProductAllocation(B, 1, List.of(new Take(B, 0, 1)), 0);

        assertThat(Allocation.decide(List.of(a, b))).isEqualTo(new Reserved(List.of(
                new Take(A, 0, 3), new Take(A, 1, 2), new Take(B, 0, 1))));
    }

    @Test
    void reportsEveryShortageAndNoTakes() {
        var a = new ProductAllocation(A, 5, List.of(new Take(A, 0, 3)), 2);
        var b = new ProductAllocation(B, 1, List.of(new Take(B, 0, 1)), 0);
        var c = new ProductAllocation(UUID.randomUUID(), 4, List.of(), 4);

        var outcome = Allocation.decide(List.of(a, b, c));

        assertThat(outcome).isInstanceOf(Insufficient.class);
        assertThat(((Insufficient) outcome).shortages()).containsExactly(
                new Shortage(A, 5, 2), new Shortage(c.productId(), 4, 4));
    }
}
```

- [ ] **Step 2: Run them and see them fail**

```bash
cd inventory-service && mvn -B -q test -Dtest='StockNeedsTest,AllocationTest'
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... StockNeeds` / `Allocation`.

- [ ] **Step 3: Implement. `StockNeeds.merge` sums duplicates and sorts by product id; `Allocation.decide` turns the per-product results into `Reserved` (all takes) or `Insufficient` (every shortage)**

Create `inventory-service/src/main/java/com/shop/inventory/domain/StockNeeds.java`:

```java
package com.shop.inventory.domain;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The quantities an order asks for, one entry per product, sorted by product id. Pure.
 * Sorting makes every order lock products in the same sequence, which rules out deadlocks between
 * products.
 */
public final class StockNeeds {

    private StockNeeds() {
    }

    public record Line(UUID productId, long quantity) {
    }

    /** @throws IllegalArgumentException if a quantity is not positive (a malformed command, not retryable) */
    public static Map<UUID, Long> merge(List<Line> lines) {
        Map<UUID, Long> needs = new TreeMap<>();
        for (Line line : lines) {
            if (line.quantity() <= 0) {
                throw new IllegalArgumentException(
                        "Quantity must be positive for product " + line.productId() + ": " + line.quantity());
            }
            needs.merge(line.productId(), line.quantity(), Long::sum);
        }
        return needs;
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/domain/Allocation.java`:

```java
package com.shop.inventory.domain;

import java.util.List;
import java.util.UUID;

/** Result of trying to cover one order's needs from the buckets. */
public final class Allocation {

    private Allocation() {
    }

    /** {@code quantity} units taken from one bucket. */
    public record Take(UUID productId, int bucketNo, long quantity) {
    }

    /** What happened for one product: the takes made and what could not be covered. */
    public record ProductAllocation(UUID productId, long requested, List<Take> takes, long missing) {

        public boolean covered() {
            return missing == 0;
        }
    }

    public record Shortage(UUID productId, long requested, long missing) {
    }

    public sealed interface Outcome permits Reserved, Insufficient {
    }

    public record Reserved(List<Take> takes) implements Outcome {
    }

    public record Insufficient(List<Shortage> shortages) implements Outcome {
    }

    /** Pure decision: all products covered, or the list of every shortage. */
    public static Outcome decide(List<ProductAllocation> allocations) {
        List<Shortage> shortages = allocations.stream()
                .filter(a -> !a.covered())
                .map(a -> new Shortage(a.productId(), a.requested(), a.missing()))
                .toList();
        if (shortages.isEmpty()) {
            return new Reserved(allocations.stream().flatMap(a -> a.takes().stream()).toList());
        }
        return new Insufficient(shortages);
    }
}
```

- [ ] **Step 4: Run them and see them pass**

```bash
cd inventory-service && mvn -B test -Dtest='StockNeedsTest,AllocationTest'
```

Expected: `Tests run: 5, Failures: 0, Errors: 0` (3 + 2).

- [ ] **Step 5: Commit**

```bash
git add inventory-service
git commit -m "feat(inventory): merge order needs and decide allocation outcome"
```

### Task 4: Admin request validation

**Files:**

- Create: `inventory-service/src/test/java/com/shop/inventory/web/RequestValidationTest.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/web/{StockDtos,RequestValidation}.java`

- [ ] **Step 1: Write the failing test**

Create `inventory-service/src/test/java/com/shop/inventory/web/RequestValidationTest.java`:

```java
package com.shop.inventory.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.shop.inventory.web.StockDtos.SetStockRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    @Test
    void acceptsValidRequests() {
        assertThat(RequestValidation.check(new SetStockRequest(0L, 1))).isEmpty();
        assertThat(RequestValidation.check(new SetStockRequest(1_000_000L, 256))).isEmpty();
    }

    @Test
    void listsEveryViolation() {
        assertThat(RequestValidation.check(new SetStockRequest(null, null))).containsExactly(
                "quantity is required and must be >= 0",
                "bucketCount is required and must be between 1 and 256");
        assertThat(RequestValidation.check(new SetStockRequest(-1L, 257))).hasSize(2);
        assertThat(RequestValidation.check(new SetStockRequest(5L, 0))).hasSize(1);
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd inventory-service && mvn -B -q test -Dtest=RequestValidationTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... RequestValidation`.

- [ ] **Step 3: Implement**

Create `inventory-service/src/main/java/com/shop/inventory/web/StockDtos.java`:

```java
package com.shop.inventory.web;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.repository.StockRepository.BucketRow;
import com.shop.inventory.repository.StockRepository.StockOverview;

/** Wire format of the admin HTTP API. */
public final class StockDtos {

    private StockDtos() {
    }

    public record SetStockRequest(Long quantity, Integer bucketCount) {
    }

    public record BucketResponse(int bucketNo, long onHand, long reserved, long free) {
    }

    /** Displayed availability is the sum over buckets (eventually consistent under load). */
    public record StockResponse(UUID productId, int bucketCount, long onHand, long reserved, long free,
                                List<BucketResponse> buckets) {

        public static StockResponse from(UUID productId, StockOverview overview) {
            List<BucketResponse> buckets = overview.buckets().stream().map(StockDtos::bucket).toList();
            return new StockResponse(
                    productId,
                    overview.bucketCount(),
                    buckets.stream().mapToLong(BucketResponse::onHand).sum(),
                    buckets.stream().mapToLong(BucketResponse::reserved).sum(),
                    buckets.stream().mapToLong(BucketResponse::free).sum(),
                    buckets);
        }
    }

    private static BucketResponse bucket(BucketRow row) {
        return new BucketResponse(row.bucketNo(), row.available(), row.reserved(), row.available() - row.reserved());
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/web/RequestValidation.java`:

```java
package com.shop.inventory.web;

import java.util.ArrayList;
import java.util.List;

import com.shop.inventory.web.StockDtos.SetStockRequest;

/** Pure request checks: the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_BUCKETS = 256;

    private RequestValidation() {
    }

    static List<String> check(SetStockRequest request) {
        List<String> errors = new ArrayList<>();
        if (request.quantity() == null || request.quantity() < 0) {
            errors.add("quantity is required and must be >= 0");
        }
        if (request.bucketCount() == null || request.bucketCount() < 1 || request.bucketCount() > MAX_BUCKETS) {
            errors.add("bucketCount is required and must be between 1 and " + MAX_BUCKETS);
        }
        return errors;
    }
}
```

- [ ] **Step 4: Run all unit tests**

```bash
cd inventory-service && mvn -B test
```

Expected: `Tests run: 17, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add inventory-service
git commit -m "feat(inventory): admin request DTOs and validation"
```

### Task 5: Admin API integration test (written first)

**Files:**

- Create: `inventory-service/src/test/java/com/shop/inventory/support/{PostgresSupport,KafkaSupport,KafkaTestClient}.java`
- Create: `inventory-service/src/test/java/com/shop/inventory/StockApiIT.java`

- [ ] **Step 1: Create the container support (PostgreSQL 18 and Kafka, one of each per JVM)**

Create `inventory-service/src/test/java/com/shop/inventory/support/PostgresSupport.java`:

```java
package com.shop.inventory.support;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.postgresql.PostgreSQLContainer;

/** One PostgreSQL container shared by every integration test of the JVM (stopped by Testcontainers at exit). */
public final class PostgresSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("inventory_it")
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

Create `inventory-service/src/test/java/com/shop/inventory/support/KafkaSupport.java`:

```java
package com.shop.inventory.support;

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

Create `inventory-service/src/test/java/com/shop/inventory/support/KafkaTestClient.java`:

```java
package com.shop.inventory.support;

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

    public void createTopics(String... topics) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
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

- [ ] **Step 2: Create the API test: bucket arithmetic (10 over 4 gives 3, 3, 2, 2), growing and shrinking the bucket count, `ADJUST` movements, 404 and 400, a quantity below the reserved amount, raising the quantity with reservations in place, and refusing to drop a bucket that still holds reservations. `shop.kafka.bootstrap-servers=false` turns the Kafka beans off, so this test needs no broker**

Create `inventory-service/src/test/java/com/shop/inventory/StockApiIT.java`:

```java
package com.shop.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.inventory.service.ReservationService;
import com.shop.inventory.support.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Whole service against a real PostgreSQL: Flyway, pool, admin routes, bucket arithmetic, conflicts. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class StockApiIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        // "false" switches the Kafka beans off (@ConditionalOnProperty), so this test needs no broker
        registry.add("shop.kafka.bootstrap-servers", () -> "false");
    }

    @Autowired
    WebTestClient client;

    @Autowired
    DatabaseClient db;

    @Autowired
    ReservationService reservations;

    @Autowired
    TransactionalOperator transactions;

    record SetBody(Long quantity, Integer bucketCount) {
    }

    @BeforeEach
    void clean() {
        db.sql("TRUNCATE product_stock_config, stock_buckets, stock_reservations, stock_movements")
                .fetch().rowsUpdated().block();
    }

    private WebTestClient.ResponseSpec put(UUID productId, Long quantity, Integer buckets) {
        return client.put().uri("/stock/{id}", productId).bodyValue(new SetBody(quantity, buckets)).exchange();
    }

    @Test
    void splitsTheQuantityOverTheBuckets() {
        UUID product = UUID.randomUUID();

        put(product, 10L, 4).expectStatus().isOk()
                .expectBody()
                .jsonPath("$.bucketCount").isEqualTo(4)
                .jsonPath("$.onHand").isEqualTo(10)
                .jsonPath("$.reserved").isEqualTo(0)
                .jsonPath("$.free").isEqualTo(10)
                .jsonPath("$.buckets[0].onHand").isEqualTo(3)
                .jsonPath("$.buckets[1].onHand").isEqualTo(3)
                .jsonPath("$.buckets[2].onHand").isEqualTo(2)
                .jsonPath("$.buckets[3].onHand").isEqualTo(2);

        client.get().uri("/stock/{id}", product).exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.free").isEqualTo(10);
    }

    @Test
    void canGrowAndShrinkTheBucketCountWhenNothingIsReserved() {
        UUID product = UUID.randomUUID();
        put(product, 100L, 1).expectStatus().isOk();

        put(product, 100L, 16).expectStatus().isOk()
                .expectBody().jsonPath("$.buckets.length()").isEqualTo(16).jsonPath("$.free").isEqualTo(100);

        put(product, 40L, 2).expectStatus().isOk()
                .expectBody().jsonPath("$.buckets.length()").isEqualTo(2)
                .jsonPath("$.onHand").isEqualTo(40).jsonPath("$.free").isEqualTo(40);
    }

    @Test
    void recordsAnAdjustMovementWithTheChangeOfFreeStock() {
        UUID product = UUID.randomUUID();
        put(product, 50L, 2).expectStatus().isOk();
        put(product, 30L, 2).expectStatus().isOk();

        List<Map<String, Object>> movements = db.sql(
                        "SELECT type, delta FROM stock_movements WHERE product_id = :p ORDER BY id")
                .bind("p", product).fetch().all().collectList().block();

        assertThat(movements).extracting(m -> m.get("type")).containsExactly("ADJUST", "ADJUST");
        assertThat(movements).extracting(m -> m.get("delta")).containsExactly(50L, -20L);
    }

    @Test
    void unknownProductIsNotFoundAndInvalidInputIsBadRequest() {
        client.get().uri("/stock/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("STOCK_NOT_FOUND");
        client.get().uri("/stock/not-a-uuid").exchange().expectStatus().isBadRequest();
        put(UUID.randomUUID(), -1L, 0).expectStatus().isBadRequest()
                .expectBody().jsonPath("$.details.length()").isEqualTo(2);
    }

    @Test
    void refusesAQuantityBelowWhatIsReservedAndChangesNothing() {
        UUID product = UUID.randomUUID();
        put(product, 10L, 1).expectStatus().isOk();
        reserve(UUID.randomUUID(), product, 6);

        put(product, 5L, 1).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("STOCK_CONFLICT");

        client.get().uri("/stock/{id}", product).exchange()
                .expectBody().jsonPath("$.onHand").isEqualTo(10).jsonPath("$.reserved").isEqualTo(6);
    }

    @Test
    void keepsReservationsWhenRaisingTheQuantity() {
        UUID product = UUID.randomUUID();
        put(product, 10L, 1).expectStatus().isOk();
        reserve(UUID.randomUUID(), product, 6);

        put(product, 20L, 1).expectStatus().isOk()
                .expectBody().jsonPath("$.onHand").isEqualTo(20)
                .jsonPath("$.reserved").isEqualTo(6)
                .jsonPath("$.free").isEqualTo(14);
    }

    @Test
    void refusesToDropABucketThatStillHoldsReservations() {
        UUID product = UUID.randomUUID();
        put(product, 8L, 4).expectStatus().isOk();
        // an order whose scan starts at the last bucket reserves there first
        UUID orderStartingAtBucket3 = new UUID(0, 3);
        reserve(orderStartingAtBucket3, product, 1);

        put(product, 8L, 2).expectStatus().isEqualTo(409);

        client.get().uri("/stock/{id}", product).exchange()
                .expectBody().jsonPath("$.bucketCount").isEqualTo(4).jsonPath("$.reserved").isEqualTo(1);
    }

    private void reserve(UUID orderId, UUID productId, long quantity) {
        transactions.transactional(reservations.reserve(orderId, Map.of(productId, quantity))).block();
    }
}
```

- [ ] **Step 3: Run it and see it fail**

```bash
cd inventory-service && mvn -B verify -Dit.test=StockApiIT
```

Expected: `COMPILATION ERROR` (`ReservationService` does not exist yet).

- [ ] **Step 4: Commit the failing test**

```bash
git add inventory-service
git commit -m "test(inventory): admin API integration test"
```

### Task 6: Repository, services and admin endpoints

**Files:**

- Create: `inventory-service/src/main/java/com/shop/inventory/domain/{StockNotFoundException,StockConflictException}.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/repository/StockRepository.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/service/{ReservationService,StockAdminService}.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/web/{InvalidRequestException,StockHandler,StockRoutes}.java`

- [ ] **Step 1: Create the domain exceptions**

Create `inventory-service/src/main/java/com/shop/inventory/domain/StockNotFoundException.java`:

```java
package com.shop.inventory.domain;

import java.util.UUID;

public class StockNotFoundException extends RuntimeException {

    public StockNotFoundException(UUID productId) {
        super("No stock defined for product " + productId);
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/domain/StockConflictException.java`:

```java
package com.shop.inventory.domain;

/** The requested stock change contradicts reservations that are still active. */
public class StockConflictException extends RuntimeException {

    public StockConflictException(String message) {
        super(message);
    }
}
```

- [ ] **Step 2: Create the repository. `take` is the heart of the design: one conditional `UPDATE ... RETURNING last_taken` per bucket (a bucket with no free stock matches no row: no write, no lock). `releaseReservations` flips the status and returns the rows in one statement**

Create `inventory-service/src/main/java/com/shop/inventory/repository/StockRepository.java`:

```java
package com.shop.inventory.repository;

import java.util.UUID;

import com.shop.inventory.domain.Allocation.Take;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL of the stock. Reservation statements are single atomic UPDATEs: the row lock lives for one
 * statement's execution plus the surrounding transaction, no SELECT ... FOR UPDATE, no read-modify-write
 * in the application.
 */
@Repository
public class StockRepository {

    private final DatabaseClient db;

    public StockRepository(DatabaseClient db) {
        this.db = db;
    }

    public record BucketRow(int bucketNo, long available, long reserved) {
    }

    public record StockOverview(int bucketCount, java.util.List<BucketRow> buckets) {
    }

    // ---- reservation side ------------------------------------------------------------------------------

    public Mono<Integer> bucketCount(UUID productId) {
        return db.sql("SELECT bucket_count FROM product_stock_config WHERE product_id = :p")
                .bind("p", productId)
                .map(row -> row.get("bucket_count", Integer.class))
                .one();
    }

    /**
     * Takes up to {@code need} units of free stock from one bucket, atomically.
     * Both SET expressions read the same locked row, so {@code last_taken} is exactly what was added to
     * {@code reserved}. A bucket with no free stock matches no row: no write, no lock.
     *
     * @return the quantity taken, 0 if the bucket has no free stock
     */
    public Mono<Long> take(UUID productId, int bucketNo, long need) {
        return db.sql("""
                        UPDATE stock_buckets
                           SET reserved   = reserved + LEAST(available - reserved, :need),
                               last_taken = LEAST(available - reserved, :need)
                         WHERE product_id = :p AND bucket_no = :b AND available > reserved
                        RETURNING last_taken""")
                .bind("need", need)
                .bind("p", productId)
                .bind("b", bucketNo)
                .map(row -> row.get("last_taken", Long.class))
                .one()
                .defaultIfEmpty(0L);
    }

    /** Gives reserved units back to the free stock of a bucket (undo of a take, or a release). */
    public Mono<Void> giveBack(Take take) {
        return db.sql("""
                        UPDATE stock_buckets SET reserved = reserved - :qty
                         WHERE product_id = :p AND bucket_no = :b""")
                .bind("qty", take.quantity())
                .bind("p", take.productId())
                .bind("b", take.bucketNo())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1
                        ? Mono.<Void>empty()
                        : Mono.error(new IllegalStateException("Bucket not found: " + take)));
    }

    public Mono<Void> insertReservation(UUID orderId, Take take) {
        return db.sql("""
                        INSERT INTO stock_reservations (order_id, product_id, bucket_no, qty, status)
                        VALUES (:o, :p, :b, :qty, 'RESERVED')""")
                .bind("o", orderId)
                .bind("p", take.productId())
                .bind("b", take.bucketNo())
                .bind("qty", take.quantity())
                .fetch().rowsUpdated().then();
    }

    /** Marks the order's active reservations as released and returns them, in one statement. */
    public Flux<Take> releaseReservations(UUID orderId) {
        return db.sql("""
                        UPDATE stock_reservations SET status = 'RELEASED', updated_at = now()
                         WHERE order_id = :o AND status = 'RESERVED'
                        RETURNING product_id, bucket_no, qty""")
                .bind("o", orderId)
                .map(row -> new Take(
                        row.get("product_id", UUID.class),
                        row.get("bucket_no", Integer.class),
                        row.get("qty", Long.class)))
                .all();
    }

    /** @param orderId the order behind the movement, {@code null} for an admin ADJUST */
    public Mono<Void> insertMovement(UUID productId, UUID orderId, long delta, String type) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO stock_movements (product_id, order_id, delta, type)
                        VALUES (:p, :o, :delta, :type)""")
                .bind("p", productId)
                .bind("delta", delta)
                .bind("type", type);
        spec = orderId == null ? spec.bindNull("o", UUID.class) : spec.bind("o", orderId);
        return spec.fetch().rowsUpdated().then();
    }

    // ---- admin side ------------------------------------------------------------------------------------

    public Mono<StockOverview> overview(UUID productId) {
        return db.sql("""
                        SELECT c.bucket_count, b.bucket_no, b.available, b.reserved
                          FROM product_stock_config c
                          JOIN stock_buckets b ON b.product_id = c.product_id
                         WHERE c.product_id = :p
                         ORDER BY b.bucket_no""")
                .bind("p", productId)
                .map(row -> new Object[] {
                        row.get("bucket_count", Integer.class),
                        new BucketRow(
                                row.get("bucket_no", Integer.class),
                                row.get("available", Long.class),
                                row.get("reserved", Long.class))})
                .all()
                .collectList()
                .filter(rows -> !rows.isEmpty())
                .map(rows -> new StockOverview(
                        (Integer) rows.get(0)[0],
                        rows.stream().map(r -> (BucketRow) r[1]).toList()));
    }

    public Mono<Long> totalReserved(UUID productId) {
        return sum("SELECT COALESCE(SUM(reserved), 0) AS total FROM stock_buckets WHERE product_id = :p", productId);
    }

    public Mono<Long> totalFree(UUID productId) {
        return sum("SELECT COALESCE(SUM(available - reserved), 0) AS total FROM stock_buckets WHERE product_id = :p",
                productId);
    }

    private Mono<Long> sum(String sql, UUID productId) {
        return db.sql(sql).bind("p", productId).map(row -> row.get("total", Long.class)).one();
    }

    public Mono<Void> upsertConfig(UUID productId, int bucketCount) {
        return db.sql("""
                        INSERT INTO product_stock_config (product_id, bucket_count) VALUES (:p, :n)
                        ON CONFLICT (product_id) DO UPDATE SET bucket_count = :n, updated_at = now()""")
                .bind("p", productId)
                .bind("n", bucketCount)
                .fetch().rowsUpdated().then();
    }

    /** Sets the FREE stock of a bucket to {@code freeShare}, keeping whatever is reserved in it. */
    public Mono<Void> upsertBucket(UUID productId, int bucketNo, long freeShare) {
        return db.sql("""
                        INSERT INTO stock_buckets (product_id, bucket_no, available, reserved)
                        VALUES (:p, :b, :share, 0)
                        ON CONFLICT (product_id, bucket_no)
                        DO UPDATE SET available = stock_buckets.reserved + :share""")
                .bind("p", productId)
                .bind("b", bucketNo)
                .bind("share", freeShare)
                .fetch().rowsUpdated().then();
    }

    /** Removes the buckets numbered {@code bucketCount} and above that hold no reservation. */
    public Mono<Void> deleteUnusedBucketsFrom(UUID productId, int bucketCount) {
        return db.sql("DELETE FROM stock_buckets WHERE product_id = :p AND bucket_no >= :n AND reserved = 0")
                .bind("p", productId)
                .bind("n", bucketCount)
                .fetch().rowsUpdated().then();
    }

    public Mono<Long> bucketsFrom(UUID productId, int bucketCount) {
        return db.sql("SELECT count(*) AS total FROM stock_buckets WHERE product_id = :p AND bucket_no >= :n")
                .bind("p", productId)
                .bind("n", bucketCount)
                .map(row -> row.get("total", Long.class))
                .one();
    }
}
```

- [ ] **Step 3: Create the reservation service. `reserve` visits the products in sorted order and, for each, the buckets in scan order (`Mono.expand` carries the remaining need); it writes reservations and movements only when everything is covered, otherwise it gives every taken unit back. `release` returns exactly the recorded takes**

Create `inventory-service/src/main/java/com/shop/inventory/service/ReservationService.java`:

```java
package com.shop.inventory.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.inventory.domain.Allocation;
import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.Outcome;
import com.shop.inventory.domain.Allocation.ProductAllocation;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.domain.Allocation.Take;
import com.shop.inventory.domain.BucketScan;
import com.shop.inventory.repository.StockRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;

/**
 * Reservation and release of stock for one order. Both methods assume they run inside the caller's
 * transaction (the message dispatcher's), so everything they write commits or rolls back together with
 * the inbox row and the outbox reply.
 */
@Service
public class ReservationService {

    private final StockRepository stock;

    public ReservationService(StockRepository stock) {
        this.stock = stock;
    }

    /**
     * Covers every need from the buckets, product by product in sorted order. If any product cannot be
     * fully covered, every unit taken so far is given back and nothing else is written: no phantom
     * reservation survives, and the caller answers with the shortages.
     *
     * @param needs quantity per product, already sorted (see {@code StockNeeds.merge})
     */
    public Mono<Outcome> reserve(UUID orderId, Map<UUID, Long> needs) {
        return Flux.fromIterable(needs.entrySet())
                .concatMap(need -> allocate(orderId, need.getKey(), need.getValue()))
                .collectList()
                .flatMap(allocations -> switch (Allocation.decide(allocations)) {
                    case Reserved reserved -> record(orderId, reserved.takes()).thenReturn((Outcome) reserved);
                    case Insufficient insufficient -> undo(allocations).thenReturn((Outcome) insufficient);
                });
    }

    /**
     * Returns exactly what the order reserved, bucket by bucket. An order with no active reservation
     * (never reserved, or already released) releases nothing and that is a success.
     *
     * @return the released takes (possibly empty)
     */
    public Mono<List<Take>> release(UUID orderId) {
        return stock.releaseReservations(orderId)
                .collectList()
                .flatMap(takes -> Flux.fromIterable(takes)
                        .concatMap(take -> stock.giveBack(take)
                                .then(stock.insertMovement(take.productId(), orderId, take.quantity(), "RELEASE")))
                        .then(Mono.just(takes)));
    }

    // One product: visit the buckets in scan order until the need is covered or the buckets run out.
    private Mono<ProductAllocation> allocate(UUID orderId, UUID productId, long need) {
        return stock.bucketCount(productId)
                .defaultIfEmpty(0)
                .flatMap(bucketCount -> Mono.just(new Scan(need, List.of(), BucketScan.order(orderId, bucketCount)))
                        .expand(scan -> scan.finished() ? Mono.empty() : step(productId, scan))
                        .last())
                .map(scan -> new ProductAllocation(productId, need, scan.takes(), scan.remaining()));
    }

    private Mono<Scan> step(UUID productId, Scan scan) {
        int bucketNo = scan.pending().get(0);
        List<Integer> rest = scan.pending().subList(1, scan.pending().size());
        return stock.take(productId, bucketNo, scan.remaining()).map(taken -> {
            List<Take> takes = new ArrayList<>(scan.takes());
            if (taken > 0) {
                takes.add(new Take(productId, bucketNo, taken));
            }
            return new Scan(scan.remaining() - taken, List.copyOf(takes), rest);
        });
    }

    /** Scan state: what is still needed, what was taken, which buckets are left to visit. */
    private record Scan(long remaining, List<Take> takes, List<Integer> pending) {

        boolean finished() {
            return remaining == 0 || pending.isEmpty();
        }
    }

    private Mono<Void> record(UUID orderId, List<Take> takes) {
        return Flux.fromIterable(takes)
                .concatMap(take -> stock.insertReservation(orderId, take)
                        .then(stock.insertMovement(take.productId(), orderId, -take.quantity(), "RESERVE")))
                .then();
    }

    private Mono<Void> undo(List<ProductAllocation> allocations) {
        return Flux.fromIterable(allocations)
                .flatMapIterable(ProductAllocation::takes)
                .concatMap(stock::giveBack)
                .then();
    }
}
```

- [ ] **Step 4: Create the admin service (one transaction: totals, config, bucket upserts, removal of unused buckets, `ADJUST` movement)**

Create `inventory-service/src/main/java/com/shop/inventory/service/StockAdminService.java`:

```java
package com.shop.inventory.service;

import java.util.UUID;

import com.shop.inventory.domain.StockConflictException;
import com.shop.inventory.domain.StockNotFoundException;
import com.shop.inventory.domain.StockShares;
import com.shop.inventory.repository.StockRepository;
import com.shop.inventory.repository.StockRepository.StockOverview;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Admin operations: set the quantity of a product and the number of buckets it is split into. */
@Service
public class StockAdminService {

    private final StockRepository stock;
    private final TransactionalOperator transactions;

    public StockAdminService(StockRepository stock, TransactionalOperator transactions) {
        this.stock = stock;
        this.transactions = transactions;
    }

    public Mono<StockOverview> get(UUID productId) {
        return stock.overview(productId).switchIfEmpty(Mono.error(() -> new StockNotFoundException(productId)));
    }

    /**
     * Sets the on-hand quantity to {@code quantity} spread over {@code bucketCount} buckets. Active
     * reservations are kept: the free stock becomes {@code quantity - reserved}, split evenly. Fails with a
     * conflict (and changes nothing) if {@code quantity} is below what is reserved, or if shrinking the
     * bucket count would drop a bucket that still holds reservations.
     */
    public Mono<StockOverview> set(UUID productId, long quantity, int bucketCount) {
        return transactions.transactional(
                        stock.totalReserved(productId).zipWith(stock.totalFree(productId))
                                .flatMap(tuple -> apply(productId, quantity, bucketCount, tuple.getT1(), tuple.getT2())))
                .then(Mono.defer(() -> get(productId)));
    }

    private Mono<Void> apply(UUID productId, long quantity, int bucketCount, long reserved, long oldFree) {
        if (quantity < reserved) {
            return Mono.error(new StockConflictException(
                    "Quantity " + quantity + " is below the " + reserved + " units currently reserved"));
        }
        long newFree = quantity - reserved;
        long[] shares = StockShares.split(newFree, bucketCount);
        return stock.upsertConfig(productId, bucketCount)
                .thenMany(Flux.range(0, bucketCount).concatMap(b -> stock.upsertBucket(productId, b, shares[b])))
                .then(stock.deleteUnusedBucketsFrom(productId, bucketCount))
                .then(stock.bucketsFrom(productId, bucketCount))
                .flatMap(left -> left > 0
                        ? Mono.<Void>error(new StockConflictException(
                                "Cannot reduce to " + bucketCount + " buckets: a removed bucket still holds reservations"))
                        : stock.insertMovement(productId, null, newFree - oldFree, "ADJUST"));
    }
}
```

- [ ] **Step 5: Create the exception type, the handlers and the routing table**

Create `inventory-service/src/main/java/com/shop/inventory/web/InvalidRequestException.java`:

```java
package com.shop.inventory.web;

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

Create `inventory-service/src/main/java/com/shop/inventory/web/StockHandler.java`:

```java
package com.shop.inventory.web;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.service.StockAdminService;
import com.shop.inventory.web.StockDtos.SetStockRequest;
import com.shop.inventory.web.StockDtos.StockResponse;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers of the admin endpoints: {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class StockHandler {

    private final StockAdminService service;

    public StockHandler(StockAdminService service) {
        this.service = service;
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> productId(request))
                .flatMap(id -> service.get(id).map(overview -> StockResponse.from(id, overview)))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> set(ServerRequest request) {
        return Mono.fromSupplier(() -> productId(request))
                .flatMap(id -> request.bodyToMono(SetStockRequest.class)
                        .switchIfEmpty(Mono.error(
                                () -> new InvalidRequestException("Request body is required", List.of())))
                        .flatMap(body -> {
                            List<String> violations = RequestValidation.check(body);
                            return violations.isEmpty()
                                    ? service.set(id, body.quantity(), body.bucketCount())
                                    : Mono.error(new InvalidRequestException("Invalid request", violations));
                        })
                        .map(overview -> StockResponse.from(id, overview)))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    private static UUID productId(ServerRequest request) {
        String raw = request.pathVariable("productId");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid product id", List.of("'" + raw + "' is not a UUID"));
        }
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/web/StockRoutes.java`:

```java
package com.shop.inventory.web;

import java.util.List;

import com.shop.inventory.domain.StockConflictException;
import com.shop.inventory.domain.StockNotFoundException;
import com.shop.inventory.web.StockDtos.ApiError;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/** The admin HTTP surface as data: a routing table plus an exception-to-status table. */
@Configuration(proxyBeanMethods = false)
public class StockRoutes {

    @Bean
    public RouterFunction<ServerResponse> stockRouter(StockHandler handler) {
        return RouterFunctions.route()
                .path("/stock", stock -> stock
                        .GET("/{productId}", handler::get)
                        .PUT("/{productId}", handler::set))
                .onError(InvalidRequestException.class, (e, r) -> problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e))
                .onError(StockNotFoundException.class, (e, r) -> problem(HttpStatus.NOT_FOUND, "STOCK_NOT_FOUND", e))
                .onError(StockConflictException.class, (e, r) -> problem(HttpStatus.CONFLICT, "STOCK_CONFLICT", e))
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

- [ ] **Step 6: Run the API test and see it pass. Docker must be running; the first run pulls `postgres:18-alpine` and `apache/kafka:4.1.0`**

```bash
cd inventory-service && mvn -B verify -Dit.test=StockApiIT
```

Expected: `Tests run: 7, Failures: 0, Errors: 0` for `StockApiIT`, `BUILD SUCCESS`. `Could not find a valid Docker environment` means Docker is not running.

- [ ] **Step 7: Commit**

```bash
git add inventory-service
git commit -m "feat(inventory): bucketed stock, reservation service and admin API"
```

### Task 7: Concurrency test

**Files:**

- Create: `inventory-service/src/test/java/com/shop/inventory/ConcurrentReservationIT.java`

- [ ] **Step 1: Create the test. It exercises the promise of the design against real row locks: 150 concurrent single-unit orders on 100 units over 8 buckets (exactly 100 succeed, nothing is oversold, no bucket ever has `reserved > available`); 40 concurrent 3-unit orders on 100 units (exactly 33 succeed, 99 reserved, 1 unit stays free); and 20 reservations released concurrently (books balance to zero). Each order runs in its own transaction and is replayed on a deadlock, the way the Kafka consumer replays a message**

Create `inventory-service/src/test/java/com/shop/inventory/ConcurrentReservationIT.java`:

```java
package com.shop.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.Outcome;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.service.ReservationService;
import com.shop.inventory.service.StockAdminService;
import com.shop.inventory.support.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;

/** The promise of the bucket design: many concurrent orders never oversell and never leave stock behind. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ConcurrentReservationIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        registry.add("shop.r2dbc.pool.max-size", () -> "8");
        registry.add("shop.kafka.bootstrap-servers", () -> "false");
    }

    @Autowired
    ReservationService reservations;

    @Autowired
    StockAdminService admin;

    @Autowired
    TransactionalOperator transactions;

    @Autowired
    DatabaseClient db;

    @BeforeEach
    void clean() {
        db.sql("TRUNCATE product_stock_config, stock_buckets, stock_reservations, stock_movements")
                .fetch().rowsUpdated().block();
    }

    /** Same retry a Kafka consumer gives a message: a deadlock victim is rolled back and run again. */
    private Outcome reserveInOwnTransaction(UUID orderId, UUID product, long quantity) {
        return transactions.transactional(reservations.reserve(orderId, Map.of(product, quantity)))
                .retryWhen(Retry.max(10).filter(DeadlockLoserDataAccessException.class::isInstance))
                .block(Duration.ofSeconds(60));
    }

    private List<Outcome> runConcurrently(UUID product, int orders, long quantityEach, int parallelism) {
        return Flux.range(0, orders)
                .flatMap(i -> transactions.transactional(
                                reservations.reserve(UUID.randomUUID(), Map.of(product, quantityEach)))
                        .retryWhen(Retry.max(10).filter(DeadlockLoserDataAccessException.class::isInstance)),
                        parallelism)
                .collectList()
                .block(Duration.ofSeconds(120));
    }

    private long reservedOf(UUID product) {
        return db.sql("SELECT COALESCE(SUM(reserved), 0) FROM stock_buckets WHERE product_id = :p")
                .bind("p", product).map(r -> r.get(0, Long.class)).one().block();
    }

    @Test
    void neverSellsMoreThanTheStockOfAMultiBucketProduct() {
        UUID product = UUID.randomUUID();
        admin.set(product, 100, 8).block();

        List<Outcome> outcomes = runConcurrently(product, 150, 1, 6);

        assertThat(outcomes.stream().filter(Reserved.class::isInstance)).hasSize(100);
        assertThat(outcomes.stream().filter(Insufficient.class::isInstance)).hasSize(50);
        assertThat(reservedOf(product)).isEqualTo(100);
        assertThat(db.sql("SELECT COUNT(*) FROM stock_buckets WHERE product_id = :p AND reserved > available")
                .bind("p", product).map(r -> r.get(0, Long.class)).one().block()).isZero();
    }

    @Test
    void ordersLargerThanABucketStillNeverOversellAndLeaveOnlyAnUnsellableRemainder() {
        UUID product = UUID.randomUUID();
        admin.set(product, 100, 8).block();

        // 40 orders of 3 units against 100: at most 33 can fit, 1 unit stays free
        List<Outcome> outcomes = runConcurrently(product, 40, 3, 6);

        long reservedOrders = outcomes.stream().filter(Reserved.class::isInstance).count();
        assertThat(reservedOrders).isEqualTo(33);
        assertThat(reservedOf(product)).isEqualTo(99);
        assertThat(db.sql("SELECT COUNT(*) FROM stock_reservations WHERE status = 'RESERVED'")
                .map(r -> r.get(0, Long.class)).one().block()).isPositive();
    }

    @Test
    void releaseAndReserveInterleavedKeepTheBooksBalanced() {
        UUID product = UUID.randomUUID();
        admin.set(product, 20, 4).block();
        List<UUID> orders = IntStream.range(0, 20).mapToObj(i -> UUID.randomUUID()).toList();
        orders.forEach(o -> reserveInOwnTransaction(o, product, 1));
        assertThat(reservedOf(product)).isEqualTo(20);

        Flux.fromIterable(orders)
                .flatMap(o -> transactions.transactional(reservations.release(o))
                        .retryWhen(Retry.max(10).filter(DeadlockLoserDataAccessException.class::isInstance)), 6)
                .blockLast(Duration.ofSeconds(60));

        assertThat(reservedOf(product)).isZero();
        assertThat(db.sql("SELECT COALESCE(SUM(delta), 0) FROM stock_movements WHERE product_id = :p AND type <> 'ADJUST'")
                .bind("p", product).map(r -> r.get(0, Long.class)).one().block()).isZero();
    }
}
```

- [ ] **Step 2: Run it**

```bash
cd inventory-service && mvn -B verify -Dit.test=ConcurrentReservationIT
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`. This test is a check on the existing implementation, so it should pass at once; if the counts differ, there is an overselling or lost-update bug to fix before anything else.

- [ ] **Step 3: Commit**

```bash
git add inventory-service
git commit -m "test(inventory): concurrent reservations never oversell"
```

### Task 8: ReserveStock and ReleaseStock saga steps

**Files:**

- Create: `inventory-service/src/test/java/com/shop/inventory/StockCommandsIT.java`
- Create: `inventory-service/src/main/java/com/shop/inventory/messaging/{InventoryMessages,ReserveStockHandler,ReleaseStockHandler,InventoryMessagingConfig,InventoryCommandsListener}.java`

- [ ] **Step 1: Write the failing test. No Debezium here: the test publishes like Debezium would (headers `id`, `type`) and reads the replies from the `outbox_events` table the handlers wrote. Cases: reserve and `StockReserved`; spill over several buckets; shortage gives `StockRejected` and leaves no reservation, no movement and no reserved unit, even for the product that could be covered; unknown product is a shortage; release gives back exactly the reserved units; release without reservation still answers `StockReleased`; a redelivered event reserves once; a quantity of 0 goes to `inventory.commands.DLT` without blocking the next command**

Create `inventory-service/src/test/java/com/shop/inventory/StockCommandsIT.java`:

```java
package com.shop.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.inventory.service.StockAdminService;
import com.shop.inventory.support.KafkaSupport;
import com.shop.inventory.support.KafkaTestClient;
import com.shop.inventory.support.PostgresSupport;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Real PostgreSQL + real Kafka, no Debezium: the test publishes like Debezium would (headers {@code id}
 * and {@code type}) and reads the replies from the {@code outbox_events} table the handlers wrote.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class StockCommandsIT {

    private static final String COMMANDS = "inventory.commands";
    private static final String REPLIES = "order.saga.replies";
    private static final KafkaTestClient KAFKA = new KafkaTestClient(KafkaSupport.bootstrapServers());

    @BeforeAll
    static void createTopics() throws Exception {
        KAFKA.createTopics(COMMANDS, COMMANDS + ".DLT");
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        registry.add("shop.kafka.bootstrap-servers", KafkaSupport::bootstrapServers);
        registry.add("shop.kafka.group-id", () -> "inventory-it-" + UUID.randomUUID());
        registry.add("shop.kafka.concurrency", () -> "1");
        registry.add("shop.kafka.retry.initial-interval", () -> "50ms");
        registry.add("shop.kafka.retry.max-interval", () -> "100ms");
        registry.add("shop.kafka.retry.max-attempts", () -> "2");
    }

    @Autowired
    StockAdminService admin;

    @Autowired
    DatabaseClient db;

    @Autowired
    JsonMapper json;

    @BeforeEach
    void clean() {
        db.sql("TRUNCATE product_stock_config, stock_buckets, stock_reservations, stock_movements, "
                        + "outbox_events, processed_events")
                .fetch().rowsUpdated().block();
    }

    private UUID stockOf(long quantity, int buckets) {
        UUID product = UUID.randomUUID();
        admin.set(product, quantity, buckets).block();
        return product;
    }

    private static String items(Object... productAndQuantity) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < productAndQuantity.length; i += 2) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"productId\":\"%s\",\"quantity\":%s}".formatted(productAndQuantity[i], productAndQuantity[i + 1]));
        }
        return sb.toString();
    }

    private static String command(UUID orderId, String data) {
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"%s\",\"data\":%s}"
                .formatted(orderId, orderId, REPLIES, data);
    }

    private void reserve(UUID orderId, UUID eventId, Object... productAndQuantity) throws Exception {
        KAFKA.send(COMMANDS, orderId.toString(), eventId, "ReserveStock",
                command(orderId, "{\"items\":[" + items(productAndQuantity) + "]}"));
    }

    private void release(UUID orderId) throws Exception {
        KAFKA.send(COMMANDS, orderId.toString(), UUID.randomUUID(), "ReleaseStock",
                command(orderId, "{\"reason\":\"ORDER_CANCELLED\"}"));
    }

    private List<Map<String, Object>> outboxRows() {
        return db.sql("SELECT topic, aggregatetype, aggregateid, type, payload::text AS payload "
                        + "FROM outbox_events ORDER BY occurred_at, id")
                .fetch().all().collectList().block();
    }

    private Map<String, Object> awaitReply(UUID orderId, String type) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(() ->
                outboxRows().stream().anyMatch(r -> orderId.toString().equals(r.get("aggregateid"))
                        && type.equals(r.get("type"))));
        return outboxRows().stream()
                .filter(r -> orderId.toString().equals(r.get("aggregateid")) && type.equals(r.get("type")))
                .findFirst().orElseThrow();
    }

    private JsonNode data(Map<String, Object> row) {
        return json.readTree((String) row.get("payload")).get("data");
    }

    private long scalar(String sql, UUID bind) {
        return db.sql(sql).bind("p", bind).map(r -> r.get(0, Long.class)).one().block();
    }

    private long reservedOf(UUID product) {
        return scalar("SELECT COALESCE(SUM(reserved), 0) FROM stock_buckets WHERE product_id = :p", product);
    }

    @Test
    void reservesAndAnswersStockReserved() throws Exception {
        UUID product = stockOf(10, 1);
        UUID order = UUID.randomUUID();

        reserve(order, UUID.randomUUID(), product, 4);

        Map<String, Object> reply = awaitReply(order, "StockReserved");
        assertThat(reply.get("topic")).isEqualTo(REPLIES);
        assertThat(reply.get("aggregatetype")).isEqualTo("Order");
        JsonNode reservations = data(reply).get("reservations");
        assertThat(reservations).hasSize(1);
        assertThat(reservations.get(0).get("productId").asString()).isEqualTo(product.toString());
        assertThat(reservations.get(0).get("quantity").asLong()).isEqualTo(4);

        assertThat(reservedOf(product)).isEqualTo(4);
        assertThat(scalar("SELECT COUNT(*) FROM stock_reservations WHERE product_id = :p AND status = 'RESERVED'",
                product)).isEqualTo(1);
        assertThat(scalar("SELECT COALESCE(SUM(delta), 0) FROM stock_movements WHERE type = 'RESERVE' AND product_id = :p",
                product)).isEqualTo(-4);
    }

    @Test
    void spillsOverSeveralBucketsWhenOneCannotCoverTheOrder() throws Exception {
        UUID product = stockOf(10, 4); // buckets 3, 3, 2, 2
        UUID order = UUID.randomUUID();

        reserve(order, UUID.randomUUID(), product, 7);

        JsonNode reservations = data(awaitReply(order, "StockReserved")).get("reservations");
        long total = 0;
        for (JsonNode r : reservations) {
            total += r.get("quantity").asLong();
        }
        assertThat(total).isEqualTo(7);
        assertThat(reservations.size()).isGreaterThan(1);
        assertThat(reservedOf(product)).isEqualTo(7);
    }

    @Test
    void answersStockRejectedAndLeavesNoPhantomReservationWhenStockIsShort() throws Exception {
        UUID plenty = stockOf(100, 1);
        UUID scarce = stockOf(5, 2);
        UUID order = UUID.randomUUID();

        reserve(order, UUID.randomUUID(), plenty, 10, scarce, 9);

        Map<String, Object> reply = awaitReply(order, "StockRejected");
        JsonNode shortages = data(reply).get("shortages");
        assertThat(shortages).hasSize(1);
        assertThat(shortages.get(0).get("productId").asString()).isEqualTo(scarce.toString());
        assertThat(shortages.get(0).get("requested").asLong()).isEqualTo(9);
        assertThat(shortages.get(0).get("missing").asLong()).isEqualTo(4);

        // the product that could be covered was given back too: nothing at all is reserved
        assertThat(reservedOf(plenty)).isZero();
        assertThat(reservedOf(scarce)).isZero();
        assertThat(db.sql("SELECT COUNT(*) FROM stock_reservations").map(r -> r.get(0, Long.class)).one().block())
                .isZero();
        assertThat(db.sql("SELECT COUNT(*) FROM stock_movements WHERE type = 'RESERVE'")
                .map(r -> r.get(0, Long.class)).one().block()).isZero();
    }

    @Test
    void anUnknownProductIsAShortage() throws Exception {
        UUID unknown = UUID.randomUUID();
        UUID order = UUID.randomUUID();

        reserve(order, UUID.randomUUID(), unknown, 1);

        JsonNode shortages = data(awaitReply(order, "StockRejected")).get("shortages");
        assertThat(shortages.get(0).get("missing").asLong()).isEqualTo(1);
    }

    @Test
    void releaseGivesBackExactlyWhatWasReserved() throws Exception {
        UUID product = stockOf(10, 4);
        UUID order = UUID.randomUUID();
        reserve(order, UUID.randomUUID(), product, 7);
        awaitReply(order, "StockReserved");

        release(order);

        JsonNode released = data(awaitReply(order, "StockReleased")).get("released");
        long total = 0;
        for (JsonNode r : released) {
            total += r.get("quantity").asLong();
        }
        assertThat(total).isEqualTo(7);
        assertThat(reservedOf(product)).isZero();
        assertThat(scalar("SELECT COUNT(*) FROM stock_reservations WHERE product_id = :p AND status = 'RELEASED'",
                product)).isPositive();
        assertThat(scalar("SELECT COALESCE(SUM(delta), 0) FROM stock_movements WHERE product_id = :p AND type IN ('RESERVE','RELEASE')",
                product)).isZero();
    }

    @Test
    void releasingAnOrderWithoutReservationStillAnswersStockReleased() throws Exception {
        UUID order = UUID.randomUUID();

        release(order);

        assertThat(data(awaitReply(order, "StockReleased")).get("released")).isEmpty();
    }

    @Test
    void aRedeliveredReserveStockReservesOnce() throws Exception {
        UUID product = stockOf(10, 1);
        UUID order = UUID.randomUUID();
        UUID sameEvent = UUID.randomUUID();

        reserve(order, sameEvent, product, 3);
        reserve(order, sameEvent, product, 3);
        UUID marker = UUID.randomUUID();
        reserve(marker, UUID.randomUUID(), product, 1); // same partition: once answered, the duplicate was consumed

        awaitReply(marker, "StockReserved");
        assertThat(reservedOf(product)).isEqualTo(4);
        assertThat(outboxRows()).hasSize(2);
    }

    @Test
    void aCommandWithANonPositiveQuantityIsDeadLetteredAndDoesNotBlockTheNext() throws Exception {
        UUID product = stockOf(10, 1);
        UUID bad = UUID.randomUUID();
        reserve(bad, UUID.randomUUID(), product, 0);
        UUID good = UUID.randomUUID();
        reserve(good, UUID.randomUUID(), product, 2);

        assertThat(KAFKA.readUntil(COMMANDS + ".DLT", r -> bad.toString().equals(r.key()), Duration.ofSeconds(30)))
                .isPresent();
        awaitReply(good, "StockReserved");
        assertThat(reservedOf(product)).isEqualTo(2);
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd inventory-service && mvn -B verify -Dit.test=StockCommandsIT
```

Expected: `ConditionTimeout` (no handler consumes `inventory.commands` yet).

- [ ] **Step 3: Create the message contract**

Create `inventory-service/src/main/java/com/shop/inventory/messaging/InventoryMessages.java`:

```java
package com.shop.inventory.messaging;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.domain.Allocation;

/** Topics, message types and payloads (the {@code data} part of the envelope) of the Inventory service. */
public final class InventoryMessages {

    public static final String COMMANDS_TOPIC = "inventory.commands";
    public static final String RESERVE_STOCK = "ReserveStock";
    public static final String RELEASE_STOCK = "ReleaseStock";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";
    public static final String STOCK_RELEASED = "StockReleased";

    private InventoryMessages() {
    }

    public record ReserveStockData(List<Item> items) {

        public record Item(UUID productId, long quantity) {
        }
    }

    /** {@code reason} is informational (for example {@code ORDER_CANCELLED}); it may be absent. */
    public record ReleaseStockData(String reason) {
    }

    public record StockReservedData(List<Allocation.Take> reservations) {
    }

    public record StockRejectedData(List<Allocation.Shortage> shortages) {
    }

    public record StockReleasedData(List<Allocation.Take> released) {
    }
}
```

- [ ] **Step 4: Create the handlers. A shortage is an answer, not an exception: `ReserveStockHandler` writes `StockRejected` through the outbox. Both run inside the dispatcher's transaction**

Create `inventory-service/src/main/java/com/shop/inventory/messaging/ReserveStockHandler.java`:

```java
package com.shop.inventory.messaging;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.Outcome;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.domain.StockNeeds;
import com.shop.inventory.messaging.InventoryMessages.ReserveStockData;
import com.shop.inventory.messaging.InventoryMessages.StockRejectedData;
import com.shop.inventory.messaging.InventoryMessages.StockReservedData;
import com.shop.inventory.service.ReservationService;
import reactor.core.publisher.Mono;

/**
 * Saga step "ReserveStock". Business refusal (not enough stock) is an answer, never an exception: the
 * reservation service leaves the stock untouched and this handler writes {@code StockRejected}. Runs
 * inside the dispatcher's transaction, so inbox row, stock changes and outbox reply commit together.
 * The order id is the saga id (spec: {@code saga_id = order_id}).
 */
public class ReserveStockHandler implements Handler {

    static final String AGGREGATE_TYPE = "Order";

    private final ReservationService reservations;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ReserveStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        this.reservations = reservations;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ReserveStockData> command = message.envelope(codec, ReserveStockData.class);
        UUID orderId = command.sagaId();
        Map<UUID, Long> needs = needs(command);
        String key = message.key() != null ? message.key() : orderId.toString();

        return reservations.reserve(orderId, needs)
                .flatMap(outcome -> reply(key, command, outcome))
                .then();
    }

    private Mono<UUID> reply(String key, Envelope<ReserveStockData> command, Outcome outcome) {
        return switch (outcome) {
            case Reserved ok -> outbox.write(command.replyTo(), AGGREGATE_TYPE, key,
                    InventoryMessages.STOCK_RESERVED, command.withData(new StockReservedData(ok.takes())));
            case Insufficient ko -> outbox.write(command.replyTo(), AGGREGATE_TYPE, key,
                    InventoryMessages.STOCK_REJECTED, command.withData(new StockRejectedData(ko.shortages())));
        };
    }

    private static Map<UUID, Long> needs(Envelope<ReserveStockData> command) {
        if (command.sagaId() == null) {
            throw new NonRetryableException("ReserveStock without sagaId");
        }
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ReserveStock without replyTo");
        }
        ReserveStockData data = command.data();
        if (data == null || data.items() == null || data.items().isEmpty()) {
            throw new NonRetryableException("ReserveStock without items");
        }
        try {
            List<StockNeeds.Line> lines = data.items().stream()
                    .map(i -> new StockNeeds.Line(i.productId(), i.quantity()))
                    .toList();
            return StockNeeds.merge(lines);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableException(e.getMessage(), e);
        }
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/messaging/ReleaseStockHandler.java`:

```java
package com.shop.inventory.messaging;

import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.inventory.messaging.InventoryMessages.ReleaseStockData;
import com.shop.inventory.messaging.InventoryMessages.StockReleasedData;
import com.shop.inventory.service.ReservationService;
import reactor.core.publisher.Mono;

/**
 * Saga compensation "ReleaseStock": gives back exactly what the order reserved. An order with nothing
 * reserved is a no-op that still answers {@code StockReleased}, so the saga can always finish.
 */
public class ReleaseStockHandler implements Handler {

    private final ReservationService reservations;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ReleaseStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        this.reservations = reservations;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ReleaseStockData> command = message.envelope(codec, ReleaseStockData.class);
        if (command.sagaId() == null) {
            throw new NonRetryableException("ReleaseStock without sagaId");
        }
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ReleaseStock without replyTo");
        }
        UUID orderId = command.sagaId();
        String key = message.key() != null ? message.key() : orderId.toString();

        return reservations.release(orderId)
                .flatMap(released -> outbox.write(command.replyTo(), ReserveStockHandler.AGGREGATE_TYPE, key,
                        InventoryMessages.STOCK_RELEASED, command.withData(new StockReleasedData(released))))
                .then();
    }
}
```

- [ ] **Step 5: Wire the consumer (registry with both types, dispatcher with consumer name `inventory-service.inventory.commands`, single listener)**

Create `inventory-service/src/main/java/com/shop/inventory/messaging/InventoryMessagingConfig.java`:

```java
package com.shop.inventory.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.outbox.OutboxWriter;
import com.shop.inventory.service.ReservationService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: which handler answers which command type on {@code inventory.commands}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class InventoryMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "inventory-service." + InventoryMessages.COMMANDS_TOPIC;

    @Bean
    public ReserveStockHandler reserveStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        return new ReserveStockHandler(reservations, outbox, codec);
    }

    @Bean
    public ReleaseStockHandler releaseStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        return new ReleaseStockHandler(reservations, outbox, codec);
    }

    @Bean
    public HandlerRegistry inventoryCommandHandlers(ReserveStockHandler reserve, ReleaseStockHandler release) {
        return HandlerRegistry.builder()
                .on(InventoryMessages.RESERVE_STOCK, reserve)
                .on(InventoryMessages.RELEASE_STOCK, release)
                .build();
    }

    @Bean
    public MessageDispatcher inventoryCommandsDispatcher(
            HandlerRegistry inventoryCommandHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(
                CONSUMER_NAME, inventoryCommandHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
```

Create `inventory-service/src/main/java/com/shop/inventory/messaging/InventoryCommandsListener.java`:

```java
package com.shop.inventory.messaging;

import com.shop.common.config.KafkaConsumerConfig;
import com.shop.common.messaging.MessageDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** The only Kafka entry point of the service: hands every record to the dispatcher. */
@Component
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class InventoryCommandsListener {

    private final MessageDispatcher dispatcher;

    public InventoryCommandsListener(MessageDispatcher inventoryCommandsDispatcher) {
        this.dispatcher = inventoryCommandsDispatcher;
    }

    @KafkaListener(topics = InventoryMessages.COMMANDS_TOPIC, containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY)
    void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        dispatcher.onMessage(record, acknowledgment);
    }
}
```

- [ ] **Step 6: Run the whole build**

```bash
cd inventory-service && mvn -B verify
```

Expected: 17 unit tests, then `StockApiIT` 7, `ConcurrentReservationIT` 3, `StockCommandsIT` 8: `Tests run: 18, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add inventory-service
git commit -m "feat(inventory): ReserveStock and ReleaseStock saga steps"
```

### Task 9: Full loop through Debezium (manual check)

**Files:**

- Modify: none

- [ ] **Step 1: Start the infrastructure (from plan 1)**

```bash
cd infra
docker compose up -d --wait inventory-db kafka connect
docker compose up kafka-init
```

Expected: services healthy, `kafka-init` exits 0.

- [ ] **Step 2: Start the service in a second terminal. Flyway creates the stock tables and the outbox/inbox tables in `inventory_db` (host port 5443)**

```bash
cd inventory-service && mvn -B spring-boot:run
```

Expected log lines: Flyway `Successfully applied 2 migrations`, then `Started InventoryServiceApplication`.

- [ ] **Step 3: Register the inventory connector (the outbox table now exists)**

```bash
cd infra && ./scripts/register-connectors.sh inventory
```

Expected: the final JSON lists `inventory-outbox` with state `RUNNING`.

- [ ] **Step 4: Define stock for a product: 10 units over 4 buckets**

```bash
PRODUCT_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
curl -s -X PUT localhost:8082/stock/$PRODUCT_ID -H 'Content-Type: application/json' \
  -d '{"quantity":10,"bucketCount":4}'
```

Expected: HTTP 200 with `free` = 10 and four buckets (3, 3, 2, 2). Keep `PRODUCT_ID` in your shell.

- [ ] **Step 5: Send a `ReserveStock` command. There is no Order service yet, so insert it into the inventory database's own outbox: Debezium publishes it to `inventory.commands` exactly as Order's connector will**

```bash
ORDER_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
cd infra && docker compose exec -T inventory-db psql -U postgres -d inventory_db <<SQL
INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
VALUES (gen_random_uuid(), 'inventory.commands', 'Order', '$ORDER_ID', 'ReserveStock',
        '{"sagaId":"$ORDER_ID","correlationId":"$ORDER_ID","replyTo":"order.saga.replies","data":{"items":[{"productId":"$PRODUCT_ID","quantity":7}]}}');
SQL
```

- [ ] **Step 6: Read the reply**

```bash
cd infra && docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic order.saga.replies --from-beginning --max-messages 1 \
  --timeout-ms 30000 --property print.headers=true --property print.key=true
```

Expected: header `type:StockReserved`, key = the order id, `reservations` summing to 7. `curl -s localhost:8082/stock/$PRODUCT_ID` now shows `reserved` = 7.

- [ ] **Step 7: Check the other paths the same way: insert a second `ReserveStock` for 9 units (reply `StockRejected`, `missing` 6, `reserved` still 7), then a `ReleaseStock` for the first order id with reason `ORDER_CANCELLED` (reply `StockReleased`, `reserved` back to 0)**

If nothing arrives, check in this order: the service log (handler error or DLT), `curl -s localhost:8083/connectors/inventory-outbox/status`, and `inventory.commands.DLT`. This is the first time the loop runs through Debezium.

- [ ] **Step 8: Stop the service (Ctrl+C) and the stack**

```bash
cd infra && docker compose down
```

---

## Done when

- `cd inventory-service && mvn -B verify` is green (17 unit + 18 integration tests).
- The Debezium loop of Task 9 returns `StockReserved`, `StockRejected` and `StockReleased` as described.

Then continue with plan 4 (Order service: orders API, saga state machine as a pure `transition` function, history tables, timeout sweeper), which produces the commands and consumes the replies defined in plans 2 and 3.
