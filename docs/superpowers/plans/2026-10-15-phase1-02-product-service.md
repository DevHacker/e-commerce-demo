# Phase 1, plan 2: Product service

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `product-service`: the product catalogue (HTTP API with optimistic locking) and the `ValidateProducts` step of the order saga (consume `product.commands`, answer on `order.saga.replies` through the outbox).

**Architecture:** Independent Maven project depending on `shop-common` (plan 1). It contains no infrastructure configuration: pool, transactions, Flyway, inbox, outbox and the Kafka container come from the library's auto-configuration. The HTTP side uses WebFlux functional endpoints (routes as data). The saga side is a `Handler` registered in a `HandlerRegistry`; its decision is a pure function (`ProductValidation.validate`), its effects are an outbox write inside the dispatcher's transaction.

**Tech Stack:** Java 21, Spring Boot 4.1.1 (WebFlux, Actuator), R2DBC `DatabaseClient`, Flyway, PostgreSQL 18, Kafka via Spring Kafka (inside `shop-common`), Testcontainers 2.0.5.

**Spec:** `docs/superpowers/specs/2026-10-15-ecommerce-phase1-design.md` (sections 5, 6, 9, 10)

**Prerequisite:** plan 1 done, and `shop-common-1.0.0-SNAPSHOT` installed in `~/.m2` (`cd common && mvn install -DskipTests -DskipITs`). Work on a branch, for example `feat/product-service`.

## Verification status (read before starting)

The code in this plan was compiled and tested before being written here, in a scratch copy:

- 10 unit tests (`ProductValidationTest` 5, `RequestValidationTest` 5): green.
- 15 integration tests (`ProductApiIT` 10, `ValidateProductsIT` 5): green, twice in a row, against **PostgreSQL 16** and an **embedded Kafka (KRaft) broker**, because the authoring sandbox had no Docker daemon.
- `mvn install` builds the executable jar.

**Not executed yet, so Tasks 4 to 6 (Testcontainers) and Task 7 (Debezium) are the first real run of:** the Testcontainers support classes (`postgres:18-alpine`, `apache/kafka:4.1.0`), PostgreSQL 18, and the full loop through Debezium. If a step fails there, fix it before moving on.

Two things the tests taught, kept in the code on purpose:

- A handler function that throws synchronously (outside its `Mono`) is **not** reached by `RouterFunctions.route().onError(...)`: the client gets a 500. Request checks therefore run inside `Mono.fromSupplier` / `Mono.defer`.
- Flyway versions are global across locations: `shop-common` owns `V1`, so every service migration starts at `V2`.

## File structure

```
product-service/
  pom.xml
  src/main/resources/application.yml
  src/main/resources/db/migration/V2__products.sql
  src/main/java/com/shop/product/
    ProductServiceApplication
    domain/      Product, ProductValidation (pure), ProductNotFoundException,
                 VersionConflictException, DuplicateSkuException
    repository/  ProductRepository                      <- all SQL
    service/     ProductService
    web/         ProductDtos, RequestValidation (pure), InvalidRequestException,
                 ProductHandler, ProductRoutes          <- functional endpoints
    messaging/   ProductMessages, ValidateProductsHandler,
                 ProductMessagingConfig, ProductCommandsListener
  src/test/java/com/shop/product/
    domain/ProductValidationTest, web/RequestValidationTest   (unit)
    ProductApiIT, ValidateProductsIT                          (Docker)
    support/PostgresSupport, KafkaSupport, KafkaTestClient
```

Conventions: unit tests are `*Test` (Surefire, no Docker); integration tests are `*IT` (Failsafe, Docker needed). Commands run from the `ecommerce/` directory unless stated.

Message contract implemented here (the Order plan produces and consumes the same shapes):

| Direction | Topic | Type | `data` |
|---|---|---|---|
| in | `product.commands` | `ValidateProducts` | `{"items":[{"productId":"<uuid>","quantity":2}]}` |
| out | `replyTo` of the command (`order.saga.replies`) | `ProductsValidated` | `{"items":[{"productId":"<uuid>","unitPrice":10.50}]}` |
| out | `replyTo` of the command | `ProductsRejected` | `{"rejections":[{"productId":"<uuid>","reason":"NOT_FOUND\|INACTIVE"}]}` |

The Kafka key is the order id; the reply keeps it (outbox `aggregateid`). Malformed commands (unparseable JSON, no `items`, no `replyTo`) are `NonRetryableException`s: straight to `product.commands.DLT`.

---

### Task 1: Scaffold the service

**Files:**

- Create: `product-service/pom.xml`
- Create: `product-service/src/main/resources/application.yml`
- Create: `product-service/src/main/resources/db/migration/V2__products.sql`
- Create: `product-service/src/main/java/com/shop/product/ProductServiceApplication.java`

- [ ] **Step 1: Create the POM (depends on `shop-common`, WebFlux, Actuator; Failsafe runs `*IT`)**

Create `product-service/pom.xml`:

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
    <artifactId>product-service</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>product-service</name>
    <description>Product catalogue and product validation step of the order saga.</description>

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

- [ ] **Step 2: Create the configuration. Only addresses and names: pool, timeouts, retry and so on come from the library defaults**

Create `product-service/src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: product-service

server:
  port: 8081

shop:
  r2dbc:
    host: ${DB_HOST:localhost}
    port: ${DB_PORT:5441}
    database: ${DB_NAME:product_db}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:postgres}
    postgres:
      application-name: product-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:29092}
    group-id: product-service

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```

- [ ] **Step 3: Create the schema migration. `V1` belongs to `shop-common` (outbox and inbox tables)**

Create `product-service/src/main/resources/db/migration/V2__products.sql`:

```sql
-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.
CREATE TABLE products (
    id         UUID PRIMARY KEY,
    sku        VARCHAR(64)    NOT NULL,
    name       VARCHAR(200)   NOT NULL,
    price      NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    active     BOOLEAN        NOT NULL DEFAULT TRUE,
    version    BIGINT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_products_sku UNIQUE (sku)
);

CREATE INDEX idx_products_created ON products (created_at, id);
```

- [ ] **Step 4: Create the application class**

Create `product-service/src/main/java/com/shop/product/ProductServiceApplication.java`:

```java
package com.shop.product;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProductServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProductServiceApplication.class, args);
    }
}
```

- [ ] **Step 5: Compile**

```bash
cd product-service && mvn -B -q compile
```

Expected: no output, exit code 0. If `shop-common` is not found, run the prerequisite install first.

- [ ] **Step 6: Commit**

```bash
git add product-service
git commit -m "build(product): scaffold product-service"
```

### Task 2: Pure validation decision

**Files:**

- Create: `product-service/src/test/java/com/shop/product/domain/ProductValidationTest.java`
- Create: `product-service/src/main/java/com/shop/product/domain/{Product,ProductValidation}.java`

- [ ] **Step 1: Write the failing test**

Create `product-service/src/test/java/com/shop/product/domain/ProductValidationTest.java`:

```java
package com.shop.product.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.product.domain.ProductValidation.PricedLine;
import com.shop.product.domain.ProductValidation.Reason;
import com.shop.product.domain.ProductValidation.Rejected;
import com.shop.product.domain.ProductValidation.Rejection;
import com.shop.product.domain.ProductValidation.Validated;
import org.junit.jupiter.api.Test;

class ProductValidationTest {

    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();
    private static final UUID MISSING = UUID.randomUUID();

    private static Product product(UUID id, String price, boolean active) {
        return new Product(id, "sku-" + id, "name", new BigDecimal(price), active, 0);
    }

    @Test
    void pricesEveryLineWhenAllProductsAreActive() {
        var catalogue = Map.of(A, product(A, "10.50", true), B, product(B, "3.00", true));

        var result = ProductValidation.validate(List.of(A, B), catalogue);

        assertThat(result).isEqualTo(new Validated(List.of(
                new PricedLine(A, new BigDecimal("10.50")),
                new PricedLine(B, new BigDecimal("3.00")))));
    }

    @Test
    void rejectsUnknownProduct() {
        var result = ProductValidation.validate(List.of(MISSING), Map.of());

        assertThat(result).isEqualTo(new Rejected(List.of(new Rejection(MISSING, Reason.NOT_FOUND))));
    }

    @Test
    void rejectsInactiveProduct() {
        var result = ProductValidation.validate(List.of(A), Map.of(A, product(A, "1.00", false)));

        assertThat(result).isEqualTo(new Rejected(List.of(new Rejection(A, Reason.INACTIVE))));
    }

    @Test
    void reportsEveryRejectionNotOnlyTheFirst() {
        var catalogue = Map.of(A, product(A, "1.00", false), B, product(B, "2.00", true));

        var result = ProductValidation.validate(List.of(A, B, MISSING), catalogue);

        assertThat(result).isEqualTo(new Rejected(List.of(
                new Rejection(A, Reason.INACTIVE),
                new Rejection(MISSING, Reason.NOT_FOUND))));
    }

    @Test
    void reportsADuplicatedProductOnceInFirstSeenOrder() {
        var catalogue = Map.of(A, product(A, "1.00", true), B, product(B, "2.00", true));

        var result = ProductValidation.validate(List.of(B, A, B), catalogue);

        assertThat(result).isEqualTo(new Validated(List.of(
                new PricedLine(B, new BigDecimal("2.00")),
                new PricedLine(A, new BigDecimal("1.00")))));
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd product-service && mvn -B -q test -Dtest=ProductValidationTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... Product` / `ProductValidation`.

- [ ] **Step 3: Implement. `validate` does no I/O: it receives the ids and the catalogue already loaded, and returns a sealed result (all lines priced, or every rejection)**

Create `product-service/src/main/java/com/shop/product/domain/Product.java`:

```java
package com.shop.product.domain;

import java.math.BigDecimal;
import java.util.UUID;

public record Product(UUID id, String sku, String name, BigDecimal price, boolean active, long version) {
}
```

Create `product-service/src/main/java/com/shop/product/domain/ProductValidation.java`:

```java
package com.shop.product.domain;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Pure decision of the "ValidateProducts" saga step: given what the order asks for and what the
 * catalogue holds, either price every line or list every reason for refusal. No I/O.
 */
public final class ProductValidation {

    private ProductValidation() {
    }

    public enum Reason { NOT_FOUND, INACTIVE }

    public record PricedLine(UUID productId, BigDecimal unitPrice) {
    }

    public record Rejection(UUID productId, Reason reason) {
    }

    public sealed interface Result permits Validated, Rejected {
    }

    public record Validated(List<PricedLine> lines) implements Result {
    }

    public record Rejected(List<Rejection> rejections) implements Result {
    }

    /**
     * @param requested product ids of the order, duplicates allowed (reported once, in first-seen order)
     * @param catalogue products found, keyed by id; missing keys mean "unknown product"
     */
    public static Result validate(List<UUID> requested, Map<UUID, Product> catalogue) {
        Set<UUID> distinct = new LinkedHashSet<>(requested);
        List<PricedLine> priced = new ArrayList<>();
        List<Rejection> rejections = new ArrayList<>();
        for (UUID id : distinct) {
            Product product = catalogue.get(id);
            if (product == null) {
                rejections.add(new Rejection(id, Reason.NOT_FOUND));
            } else if (!product.active()) {
                rejections.add(new Rejection(id, Reason.INACTIVE));
            } else {
                priced.add(new PricedLine(id, product.price()));
            }
        }
        return rejections.isEmpty() ? new Validated(List.copyOf(priced)) : new Rejected(List.copyOf(rejections));
    }
}
```

- [ ] **Step 4: Run it and see it pass**

```bash
cd product-service && mvn -B test -Dtest=ProductValidationTest
```

Expected: `Tests run: 5, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add product-service
git commit -m "feat(product): pure product validation for the saga step"
```

### Task 3: Request DTOs and validation

**Files:**

- Create: `product-service/src/test/java/com/shop/product/web/RequestValidationTest.java`
- Create: `product-service/src/main/java/com/shop/product/web/{ProductDtos,RequestValidation}.java`

- [ ] **Step 1: Write the failing test**

Create `product-service/src/test/java/com/shop/product/web/RequestValidationTest.java`:

```java
package com.shop.product.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import com.shop.product.web.ProductDtos.CreateProductRequest;
import com.shop.product.web.ProductDtos.UpdateProductRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    @Test
    void acceptsAValidCreateRequest() {
        assertThat(RequestValidation.check(new CreateProductRequest("SKU-1", "Mug", new BigDecimal("9.99"))))
                .isEmpty();
    }

    @Test
    void listsEveryViolationOfACreateRequest() {
        assertThat(RequestValidation.check(new CreateProductRequest(" ", null, new BigDecimal("-1"))))
                .containsExactly("sku is required", "name is required", "price must be >= 0");
    }

    @Test
    void rejectsMoreThanTwoDecimalsAndHugePrices() {
        assertThat(RequestValidation.check(new CreateProductRequest("S", "N", new BigDecimal("1.005"))))
                .containsExactly("price must have at most 2 decimals");
        assertThat(RequestValidation.check(new CreateProductRequest("S", "N", new BigDecimal("12345678901"))))
                .containsExactly("price is too large");
    }

    @Test
    void rejectsTooLongSku() {
        assertThat(RequestValidation.check(new CreateProductRequest("x".repeat(65), "N", BigDecimal.ONE)))
                .containsExactly("sku must be at most 64 characters");
    }

    @Test
    void updateRequiresActiveAndVersion() {
        assertThat(RequestValidation.check(new UpdateProductRequest("Mug", BigDecimal.ONE, null, null)))
                .containsExactly("active is required", "version is required and must be >= 0");
        assertThat(RequestValidation.check(new UpdateProductRequest("Mug", BigDecimal.ONE, true, 3L))).isEmpty();
    }
}
```

- [ ] **Step 2: Run it and see it fail**

```bash
cd product-service && mvn -B -q test -Dtest=RequestValidationTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... RequestValidation`.

- [ ] **Step 3: Implement. Checks return the list of violations; empty means valid**

Create `product-service/src/main/java/com/shop/product/web/ProductDtos.java`:

```java
package com.shop.product.web;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.shop.product.domain.Product;

/** Wire format of the HTTP API. */
public final class ProductDtos {

    private ProductDtos() {
    }

    public record CreateProductRequest(String sku, String name, BigDecimal price) {
    }

    public record UpdateProductRequest(String name, BigDecimal price, Boolean active, Long version) {
    }

    public record ProductResponse(UUID id, String sku, String name, BigDecimal price, boolean active, long version) {

        public static ProductResponse from(Product p) {
            return new ProductResponse(p.id(), p.sku(), p.name(), p.price(), p.active(), p.version());
        }
    }

    public record PageResponse<T>(List<T> items, int page, int size, boolean hasNext) {
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
```

Create `product-service/src/main/java/com/shop/product/web/RequestValidation.java`:

```java
package com.shop.product.web;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.shop.product.web.ProductDtos.CreateProductRequest;
import com.shop.product.web.ProductDtos.UpdateProductRequest;

/** Pure request checks: each returns the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_PAGE_SIZE = 100;

    private RequestValidation() {
    }

    static List<String> check(CreateProductRequest r) {
        List<String> errors = new ArrayList<>();
        text(errors, "sku", r.sku(), 64);
        text(errors, "name", r.name(), 200);
        price(errors, r.price());
        return errors;
    }

    static List<String> check(UpdateProductRequest r) {
        List<String> errors = new ArrayList<>();
        text(errors, "name", r.name(), 200);
        price(errors, r.price());
        if (r.active() == null) {
            errors.add("active is required");
        }
        if (r.version() == null || r.version() < 0) {
            errors.add("version is required and must be >= 0");
        }
        return errors;
    }

    private static void text(List<String> errors, String field, String value, int max) {
        if (value == null || value.isBlank()) {
            errors.add(field + " is required");
        } else if (value.length() > max) {
            errors.add(field + " must be at most " + max + " characters");
        }
    }

    private static void price(List<String> errors, BigDecimal price) {
        if (price == null) {
            errors.add("price is required");
        } else if (price.signum() < 0) {
            errors.add("price must be >= 0");
        } else if (price.scale() > 2) {
            errors.add("price must have at most 2 decimals");
        } else if (price.precision() - price.scale() > 10) {
            errors.add("price is too large");
        }
    }
}
```

- [ ] **Step 4: Run both unit test classes**

```bash
cd product-service && mvn -B test
```

Expected: `Tests run: 10, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add product-service
git commit -m "feat(product): API DTOs and pure request validation"
```

### Task 4: HTTP API integration test (written first)

**Files:**

- Create: `product-service/src/test/java/com/shop/product/support/PostgresSupport.java`
- Create: `product-service/src/test/java/com/shop/product/ProductApiIT.java`

- [ ] **Step 1: Create the container support (PostgreSQL 18, one per JVM)**

Create `product-service/src/test/java/com/shop/product/support/PostgresSupport.java`:

```java
package com.shop.product.support;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.postgresql.PostgreSQLContainer;

/** One PostgreSQL container shared by every integration test of the JVM (stopped by Testcontainers at exit). */
public final class PostgresSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("product_it")
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

- [ ] **Step 2: Create the API test. It covers create/read, Location header, duplicate SKU, validation errors, 404 and malformed id, optimistic update, 8 concurrent updates from one version (exactly one wins), paging and health. `shop.kafka.bootstrap-servers=false` turns the Kafka beans off, so this test needs no broker**

Create `product-service/src/test/java/com/shop/product/ProductApiIT.java`:

```java
package com.shop.product;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.product.support.PostgresSupport;
import com.shop.product.web.ProductDtos.ProductResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/** Whole service against a real PostgreSQL: Flyway, pool, routes, optimistic locking, error mapping. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class ProductApiIT {

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

    @BeforeEach
    void cleanTable() {
        db.sql("TRUNCATE products").fetch().rowsUpdated().block();
    }

    private ProductResponse create(String sku, String price) {
        return client.post().uri("/products")
                .bodyValue(new CreateBody(sku, "Mug " + sku, new BigDecimal(price)))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ProductResponse.class).returnResult().getResponseBody();
    }

    record CreateBody(String sku, String name, BigDecimal price) {
    }

    record UpdateBody(String name, BigDecimal price, boolean active, long version) {
    }

    @Test
    void createsThenReadsAProduct() {
        ProductResponse created = create("SKU-1", "9.99");

        assertThat(created.version()).isZero();
        assertThat(created.active()).isTrue();
        client.get().uri("/products/{id}", created.id())
                .exchange()
                .expectStatus().isOk()
                .expectBody(ProductResponse.class).isEqualTo(created);
    }

    @Test
    void returnsLocationHeaderOnCreate() {
        client.post().uri("/products")
                .bodyValue(new CreateBody("SKU-LOC", "Mug", new BigDecimal("1.00")))
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().value("Location", loc -> assertThat(loc).startsWith("/products/"));
    }

    @Test
    void duplicateSkuIsAConflict() {
        create("SKU-DUP", "1.00");

        client.post().uri("/products")
                .bodyValue(new CreateBody("SKU-DUP", "Other", new BigDecimal("2.00")))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT)
                .expectBody().jsonPath("$.code").isEqualTo("DUPLICATE_SKU");
    }

    @Test
    void invalidBodyListsEveryViolation() {
        client.post().uri("/products")
                .bodyValue(new CreateBody("", null, new BigDecimal("-5")))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo("INVALID_REQUEST")
                .jsonPath("$.details.length()").isEqualTo(3);
    }

    @Test
    void unknownProductIsNotFoundAndMalformedIdIsBadRequest() {
        client.get().uri("/products/{id}", UUID.randomUUID())
                .exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("PRODUCT_NOT_FOUND");

        client.get().uri("/products/not-a-uuid").exchange().expectStatus().isBadRequest();
    }

    @Test
    void updateBumpsTheVersionAndStaleVersionIsAConflict() {
        ProductResponse created = create("SKU-UPD", "5.00");

        ProductResponse updated = client.put().uri("/products/{id}", created.id())
                .bodyValue(new UpdateBody("Renamed", new BigDecimal("6.50"), false, created.version()))
                .exchange()
                .expectStatus().isOk()
                .expectBody(ProductResponse.class).returnResult().getResponseBody();

        assertThat(updated.version()).isEqualTo(1);
        assertThat(updated.name()).isEqualTo("Renamed");
        assertThat(updated.price()).isEqualByComparingTo("6.50");
        assertThat(updated.active()).isFalse();

        // the same (now stale) version a second time
        client.put().uri("/products/{id}", created.id())
                .bodyValue(new UpdateBody("Again", new BigDecimal("7.00"), true, created.version()))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT)
                .expectBody().jsonPath("$.code").isEqualTo("VERSION_CONFLICT");
    }

    @Test
    void updatingAnUnknownProductIsNotFound() {
        client.put().uri("/products/{id}", UUID.randomUUID())
                .bodyValue(new UpdateBody("X", BigDecimal.ONE, true, 0))
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void twoConcurrentUpdatesFromTheSameVersionLetExactlyOneWin() {
        ProductResponse created = create("SKU-RACE", "5.00");

        var results = java.util.stream.IntStream.range(0, 8).parallel()
                .mapToObj(i -> client.put().uri("/products/{id}", created.id())
                        .bodyValue(new UpdateBody("Racer " + i, new BigDecimal("1.00"), true, created.version()))
                        .exchange()
                        .returnResult(String.class).getStatus().value())
                .toList();

        assertThat(results.stream().filter(s -> s == 200)).hasSize(1);
        assertThat(results.stream().filter(s -> s == 409)).hasSize(7);
    }

    @Test
    void listsInPagesAndReportsWhetherThereIsANextPage() {
        for (int i = 1; i <= 5; i++) {
            create("SKU-P" + i, "1.00");
        }

        client.get().uri("/products?page=0&size=2").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(2)
                .jsonPath("$.hasNext").isEqualTo(true);

        client.get().uri("/products?page=2&size=2").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(1)
                .jsonPath("$.hasNext").isEqualTo(false);

        client.get().uri("/products?size=1000").exchange().expectStatus().isBadRequest();
    }

    @Test
    void healthEndpointIsUp() {
        client.get().uri("/actuator/health").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
    }
}
```

- [ ] **Step 3: Run it and see it fail. Docker must be running; the first run pulls `postgres:18-alpine`**

```bash
cd product-service && mvn -B verify -Dit.test=ProductApiIT
```

Expected: tests fail with `404` (no routes yet) or on a missing repository bean; the context itself must start (Flyway applies V1 then V2). `Could not find a valid Docker environment` means Docker is not running.

- [ ] **Step 4: Commit the failing test**

```bash
git add product-service
git commit -m "test(product): API integration test"
```

### Task 5: Repository, service and functional endpoints

**Files:**

- Create: `product-service/src/main/java/com/shop/product/domain/{ProductNotFoundException,VersionConflictException,DuplicateSkuException}.java`
- Create: `product-service/src/main/java/com/shop/product/repository/ProductRepository.java`
- Create: `product-service/src/main/java/com/shop/product/service/ProductService.java`
- Create: `product-service/src/main/java/com/shop/product/web/{InvalidRequestException,ProductHandler,ProductRoutes}.java`

- [ ] **Step 1: Create the domain exceptions**

Create `product-service/src/main/java/com/shop/product/domain/ProductNotFoundException.java`:

```java
package com.shop.product.domain;

import java.util.UUID;

public class ProductNotFoundException extends RuntimeException {

    public ProductNotFoundException(UUID id) {
        super("Product " + id + " not found");
    }
}
```

Create `product-service/src/main/java/com/shop/product/domain/VersionConflictException.java`:

```java
package com.shop.product.domain;

import java.util.UUID;

/** The caller updated from a stale {@code version}: someone else changed the product first. */
public class VersionConflictException extends RuntimeException {

    public VersionConflictException(UUID id) {
        super("Product " + id + " was modified by someone else; reload and retry");
    }
}
```

Create `product-service/src/main/java/com/shop/product/domain/DuplicateSkuException.java`:

```java
package com.shop.product.domain;

public class DuplicateSkuException extends RuntimeException {

    public DuplicateSkuException(String sku) {
        super("A product with SKU '" + sku + "' already exists");
    }
}
```

- [ ] **Step 2: Create the repository. The update is `UPDATE ... WHERE id = :id AND version = :version RETURNING ...`: no `FOR UPDATE`, no connection held across calls (optimistic locking, as the spec decided)**

Create `product-service/src/main/java/com/shop/product/repository/ProductRepository.java`:

```java
package com.shop.product.repository;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.UUID;

import com.shop.product.domain.Product;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/** All SQL of the catalogue. Every method is lazy: nothing runs until subscribed. */
@Repository
public class ProductRepository {

    private static final String COLUMNS = "id, sku, name, price, active, version";

    private final DatabaseClient db;

    public ProductRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Product> insert(Product product) {
        return db.sql("""
                        INSERT INTO products (id, sku, name, price, active)
                        VALUES (:id, :sku, :name, :price, :active)
                        RETURNING""" + " " + COLUMNS)
                .bind("id", product.id())
                .bind("sku", product.sku())
                .bind("name", product.name())
                .bind("price", product.price())
                .bind("active", product.active())
                .map(ProductRepository::toProduct)
                .one();
    }

    public Mono<Product> findById(UUID id) {
        return db.sql("SELECT " + COLUMNS + " FROM products WHERE id = :id")
                .bind("id", id)
                .map(ProductRepository::toProduct)
                .one();
    }

    public Flux<Product> findByIds(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return Flux.empty();
        }
        return db.sql("SELECT " + COLUMNS + " FROM products WHERE id = ANY(:ids)")
                .bind("ids", ids.toArray(UUID[]::new))
                .map(ProductRepository::toProduct)
                .all();
    }

    public Flux<Product> findPage(int limit, long offset) {
        return db.sql("SELECT " + COLUMNS + " FROM products ORDER BY created_at, id LIMIT :limit OFFSET :offset")
                .bind("limit", limit)
                .bind("offset", offset)
                .map(ProductRepository::toProduct)
                .all();
    }

    /**
     * Optimistic update: succeeds only if the stored version equals {@code expectedVersion}.
     * Empty when the product does not exist or the version is stale.
     */
    public Mono<Product> update(UUID id, String name, BigDecimal price, boolean active, long expectedVersion) {
        return db.sql("""
                        UPDATE products
                           SET name = :name, price = :price, active = :active,
                               version = version + 1, updated_at = now()
                         WHERE id = :id AND version = :version
                        RETURNING""" + " " + COLUMNS)
                .bind("id", id)
                .bind("name", name)
                .bind("price", price)
                .bind("active", active)
                .bind("version", expectedVersion)
                .map(ProductRepository::toProduct)
                .one();
    }

    public Mono<Boolean> exists(UUID id) {
        return db.sql("SELECT 1 FROM products WHERE id = :id")
                .bind("id", id)
                .map(row -> Boolean.TRUE)
                .one()
                .defaultIfEmpty(Boolean.FALSE);
    }

    private static Product toProduct(Readable row) {
        return new Product(
                row.get("id", UUID.class),
                row.get("sku", String.class),
                row.get("name", String.class),
                row.get("price", BigDecimal.class),
                Boolean.TRUE.equals(row.get("active", Boolean.class)),
                row.get("version", Long.class));
    }
}
```

- [ ] **Step 3: Create the service. Empty update result is disambiguated: product exists means stale version (409), otherwise 404. A duplicate SKU surfaces as `DuplicateKeyException`**

Create `product-service/src/main/java/com/shop/product/service/ProductService.java`:

```java
package com.shop.product.service;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.product.domain.DuplicateSkuException;
import com.shop.product.domain.Product;
import com.shop.product.domain.ProductNotFoundException;
import com.shop.product.domain.VersionConflictException;
import com.shop.product.repository.ProductRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Service
public class ProductService {

    private final ProductRepository repository;

    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }

    public Mono<Product> create(String sku, String name, BigDecimal price) {
        Product product = new Product(UUID.randomUUID(), sku, name, price, true, 0);
        return repository.insert(product)
                .onErrorMap(DuplicateKeyException.class, e -> new DuplicateSkuException(sku));
    }

    public Mono<Product> get(UUID id) {
        return repository.findById(id).switchIfEmpty(Mono.error(() -> new ProductNotFoundException(id)));
    }

    /** @return one page plus one extra row, so the caller can tell whether a next page exists */
    public Flux<Product> list(int page, int size) {
        return repository.findPage(size + 1, (long) page * size);
    }

    public Mono<Product> update(UUID id, String name, BigDecimal price, boolean active, long expectedVersion) {
        return repository.update(id, name, price, active, expectedVersion)
                .switchIfEmpty(Mono.defer(() -> repository.exists(id)
                        .flatMap(exists -> exists
                                ? Mono.<Product>error(new VersionConflictException(id))
                                : Mono.<Product>error(new ProductNotFoundException(id)))));
    }
}
```

- [ ] **Step 4: Create the exception type, the handlers and the routing table. Handlers are `ServerRequest -> Mono<ServerResponse>`; the exception-to-status table is part of the router**

Create `product-service/src/main/java/com/shop/product/web/InvalidRequestException.java`:

```java
package com.shop.product.web;

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

Create `product-service/src/main/java/com/shop/product/web/ProductHandler.java`:

```java
package com.shop.product.web;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.shop.product.service.ProductService;
import com.shop.product.web.ProductDtos.CreateProductRequest;
import com.shop.product.web.ProductDtos.PageResponse;
import com.shop.product.web.ProductDtos.ProductResponse;
import com.shop.product.web.ProductDtos.UpdateProductRequest;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers of the functional endpoints: each one is {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class ProductHandler {

    private final ProductService service;

    public ProductHandler(ProductService service) {
        this.service = service;
    }

    public Mono<ServerResponse> create(ServerRequest request) {
        return request.bodyToMono(CreateProductRequest.class)
                .switchIfEmpty(Mono.error(() -> new InvalidRequestException("Request body is required", List.of())))
                .flatMap(body -> validated(body, RequestValidation.check(body)))
                .flatMap(body -> service.create(body.sku().trim(), body.name().trim(), body.price()))
                .map(ProductResponse::from)
                .flatMap(created -> ServerResponse.created(URI.create("/products/" + created.id()))
                        .bodyValue(created));
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> pathId(request))
                .flatMap(service::get)
                .map(ProductResponse::from)
                .flatMap(product -> ServerResponse.ok().bodyValue(product));
    }

    public Mono<ServerResponse> list(ServerRequest request) {
        return Mono.fromSupplier(() -> new PageParams(
                        intParam(request, "page", 0, 0, Integer.MAX_VALUE / 2),
                        intParam(request, "size", 20, 1, RequestValidation.MAX_PAGE_SIZE)))
                .flatMap(params -> service.list(params.page(), params.size())
                        .map(ProductResponse::from)
                        .collectList()
                        .map(rows -> new PageResponse<>(
                                rows.size() > params.size() ? rows.subList(0, params.size()) : rows,
                                params.page(), params.size(), rows.size() > params.size())))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> update(ServerRequest request) {
        return Mono.fromSupplier(() -> pathId(request))
                .flatMap(id -> request.bodyToMono(UpdateProductRequest.class)
                        .switchIfEmpty(Mono.error(
                                () -> new InvalidRequestException("Request body is required", List.of())))
                        .flatMap(body -> validated(body, RequestValidation.check(body)))
                        .flatMap(body -> service.update(
                                id, body.name().trim(), body.price(), body.active(), body.version())))
                .map(ProductResponse::from)
                .flatMap(product -> ServerResponse.ok().bodyValue(product));
    }

    private record PageParams(int page, int size) {
    }

    private static <T> Mono<T> validated(T body, List<String> violations) {
        return violations.isEmpty()
                ? Mono.just(body)
                : Mono.error(new InvalidRequestException("Invalid request", violations));
    }

    private static UUID pathId(ServerRequest request) {
        String raw = request.pathVariable("id");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid product id", List.of("'" + raw + "' is not a UUID"));
        }
    }

    private static int intParam(ServerRequest request, String name, int defaultValue, int min, int max) {
        return request.queryParam(name).map(raw -> {
            try {
                int value = Integer.parseInt(raw);
                if (value < min || value > max) {
                    throw new NumberFormatException();
                }
                return value;
            } catch (NumberFormatException e) {
                throw new InvalidRequestException("Invalid query parameter",
                        List.of(name + " must be an integer between " + min + " and " + max));
            }
        }).orElse(defaultValue);
    }
}
```

Create `product-service/src/main/java/com/shop/product/web/ProductRoutes.java`:

```java
package com.shop.product.web;

import java.util.List;

import com.shop.product.domain.DuplicateSkuException;
import com.shop.product.domain.ProductNotFoundException;
import com.shop.product.domain.VersionConflictException;
import com.shop.product.web.ProductDtos.ApiError;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/** The HTTP surface as data: a routing table plus a table of exception-to-status mappings. */
@Configuration(proxyBeanMethods = false)
public class ProductRoutes {

    @Bean
    public RouterFunction<ServerResponse> productRouter(ProductHandler handler) {
        return RouterFunctions.route()
                .path("/products", products -> products
                        .POST("", handler::create)
                        .GET("", handler::list)
                        .GET("/{id}", handler::get)
                        .PUT("/{id}", handler::update))
                .onError(InvalidRequestException.class, (e, r) -> problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e))
                .onError(ProductNotFoundException.class, (e, r) -> problem(HttpStatus.NOT_FOUND, "PRODUCT_NOT_FOUND", e))
                .onError(VersionConflictException.class, (e, r) -> problem(HttpStatus.CONFLICT, "VERSION_CONFLICT", e))
                .onError(DuplicateSkuException.class, (e, r) -> problem(HttpStatus.CONFLICT, "DUPLICATE_SKU", e))
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

- [ ] **Step 5: Run the API test and see it pass**

```bash
cd product-service && mvn -B verify -Dit.test=ProductApiIT
```

Expected: `Tests run: 10, Failures: 0, Errors: 0` for `ProductApiIT`, `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add product-service
git commit -m "feat(product): catalogue API with optimistic locking"
```

### Task 6: ValidateProducts saga step

**Files:**

- Create: `product-service/src/test/java/com/shop/product/support/{KafkaSupport,KafkaTestClient}.java`
- Create: `product-service/src/test/java/com/shop/product/ValidateProductsIT.java`
- Create: `product-service/src/main/java/com/shop/product/messaging/{ProductMessages,ValidateProductsHandler,ProductMessagingConfig,ProductCommandsListener}.java`

- [ ] **Step 1: Create the Kafka test support**

Create `product-service/src/test/java/com/shop/product/support/KafkaSupport.java`:

```java
package com.shop.product.support;

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

Create `product-service/src/test/java/com/shop/product/support/KafkaTestClient.java`:

```java
package com.shop.product.support;

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

- [ ] **Step 2: Write the failing test. There is no Debezium in this test: it publishes like Debezium would (headers `id`, `type`) and reads the reply from the `outbox_events` table the handler wrote. Cases: valid command gets current prices; unknown and inactive products are rejected with reasons; a redelivered event id produces one reply; unparseable JSON goes to `product.commands.DLT` without blocking the next command; a command with no items goes to the DLT**

Create `product-service/src/test/java/com/shop/product/ValidateProductsIT.java`:

```java
package com.shop.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.product.domain.Product;
import com.shop.product.service.ProductService;
import com.shop.product.support.KafkaSupport;
import com.shop.product.support.KafkaTestClient;
import com.shop.product.support.PostgresSupport;
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
 * Real PostgreSQL + real Kafka, no Debezium: the test publishes like Debezium would (headers
 * {@code id} and {@code type}) and reads the reply from the outbox table the handler wrote.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ValidateProductsIT {

    private static final String COMMANDS = "product.commands";
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
        registry.add("shop.kafka.group-id", () -> "product-it-" + UUID.randomUUID());
        registry.add("shop.kafka.concurrency", () -> "1");
        registry.add("shop.kafka.retry.initial-interval", () -> "50ms");
        registry.add("shop.kafka.retry.max-interval", () -> "100ms");
        registry.add("shop.kafka.retry.max-attempts", () -> "2");
    }

    @Autowired
    ProductService products;

    @Autowired
    DatabaseClient db;

    @Autowired
    JsonMapper json;

    @BeforeEach
    void clean() {
        db.sql("TRUNCATE products, outbox_events, processed_events").fetch().rowsUpdated().block();
    }

    private Product product(String sku, String price, boolean active) {
        Product created = products.create(sku, "Name " + sku, new BigDecimal(price)).block();
        return active ? created
                : products.update(created.id(), created.name(), created.price(), false, created.version()).block();
    }

    private static String command(UUID orderId, UUID... productIds) {
        String items = java.util.Arrays.stream(productIds)
                .map(id -> "{\"productId\":\"%s\",\"quantity\":2}".formatted(id))
                .collect(java.util.stream.Collectors.joining(","));
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"%s\",\"data\":{\"items\":[%s]}}"
                .formatted(orderId, orderId, REPLIES, items);
    }

    private void send(UUID orderId, UUID eventId, String payload) throws Exception {
        KAFKA.send(COMMANDS, orderId.toString(), eventId, "ValidateProducts", payload);
    }

    private List<Map<String, Object>> outboxRows() {
        return db.sql("SELECT topic, aggregatetype, aggregateid, type, payload::text AS payload "
                        + "FROM outbox_events ORDER BY occurred_at, id")
                .fetch().all().collectList().block();
    }

    private List<Map<String, Object>> awaitOutboxRows(int count) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> outboxRows().size() >= count);
        return outboxRows();
    }

    @Test
    void validCommandAnswersProductsValidatedWithCurrentPrices() throws Exception {
        Product a = product("A", "10.50", true);
        Product b = product("B", "3.00", true);
        UUID orderId = UUID.randomUUID();

        send(orderId, UUID.randomUUID(), command(orderId, a.id(), b.id()));

        Map<String, Object> row = awaitOutboxRows(1).get(0);
        assertThat(row.get("topic")).isEqualTo(REPLIES);
        assertThat(row.get("aggregatetype")).isEqualTo("Order");
        assertThat(row.get("aggregateid")).isEqualTo(orderId.toString());
        assertThat(row.get("type")).isEqualTo("ProductsValidated");

        JsonNode payload = json.readTree((String) row.get("payload"));
        assertThat(payload.get("sagaId").asString()).isEqualTo(orderId.toString());
        assertThat(payload.get("replyTo").asString()).isEqualTo(REPLIES);
        JsonNode items = payload.get("data").get("items");
        assertThat(items).hasSize(2);
        assertThat(items.get(0).get("productId").asString()).isEqualTo(a.id().toString());
        assertThat(items.get(0).get("unitPrice").decimalValue()).isEqualByComparingTo("10.50");
        assertThat(items.get(1).get("unitPrice").decimalValue()).isEqualByComparingTo("3.00");
    }

    @Test
    void unknownAndInactiveProductsAreRejectedWithTheirReasons() throws Exception {
        Product inactive = product("OFF", "1.00", false);
        UUID unknown = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        send(orderId, UUID.randomUUID(), command(orderId, inactive.id(), unknown));

        Map<String, Object> row = awaitOutboxRows(1).get(0);
        assertThat(row.get("type")).isEqualTo("ProductsRejected");
        JsonNode rejections = json.readTree((String) row.get("payload")).get("data").get("rejections");
        assertThat(rejections).hasSize(2);
        assertThat(rejections.get(0).get("productId").asString()).isEqualTo(inactive.id().toString());
        assertThat(rejections.get(0).get("reason").asString()).isEqualTo("INACTIVE");
        assertThat(rejections.get(1).get("productId").asString()).isEqualTo(unknown.toString());
        assertThat(rejections.get(1).get("reason").asString()).isEqualTo("NOT_FOUND");
    }

    @Test
    void aRedeliveredCommandProducesOneReplyOnly() throws Exception {
        Product a = product("A", "10.00", true);
        UUID orderId = UUID.randomUUID();
        UUID sameEvent = UUID.randomUUID();

        send(orderId, sameEvent, command(orderId, a.id()));
        send(orderId, sameEvent, command(orderId, a.id()));
        // marker: same partition, so once its reply exists the duplicate has been consumed too
        UUID markerOrder = UUID.randomUUID();
        send(markerOrder, UUID.randomUUID(), command(markerOrder, a.id()));

        List<Map<String, Object>> rows = awaitOutboxRows(2);
        assertThat(rows).extracting(r -> r.get("aggregateid"))
                .containsExactlyInAnyOrder(orderId.toString(), markerOrder.toString());
        assertThat(outboxRows()).hasSize(2);
    }

    @Test
    void anUnreadableCommandGoesToTheDeadLetterTopicAndDoesNotBlockTheNext() throws Exception {
        Product a = product("A", "10.00", true);
        UUID badOrder = UUID.randomUUID();

        send(badOrder, UUID.randomUUID(), "{ this is not json");
        UUID goodOrder = UUID.randomUUID();
        send(goodOrder, UUID.randomUUID(), command(goodOrder, a.id()));

        assertThat(KAFKA.readUntil(COMMANDS + ".DLT", r -> badOrder.toString().equals(r.key()), Duration.ofSeconds(30)))
                .isPresent();

        Map<String, Object> row = awaitOutboxRows(1).get(0);
        assertThat(row.get("aggregateid")).isEqualTo(goodOrder.toString());
        assertThat(row.get("type")).isEqualTo("ProductsValidated");
    }

    @Test
    void aCommandWithoutItemsIsDeadLettered() throws Exception {
        UUID orderId = UUID.randomUUID();

        send(orderId, UUID.randomUUID(), command(orderId));

        assertThat(KAFKA.readUntil(COMMANDS + ".DLT", r -> orderId.toString().equals(r.key()), Duration.ofSeconds(30)))
                .isPresent();
        assertThat(outboxRows()).isEmpty();
    }
}
```

- [ ] **Step 3: Run it and see it fail**

```bash
cd product-service && mvn -B verify -Dit.test=ValidateProductsIT
```

Expected: `ConditionTimeout` (no handler consumes `product.commands` yet).

- [ ] **Step 4: Create the message contract**

Create `product-service/src/main/java/com/shop/product/messaging/ProductMessages.java`:

```java
package com.shop.product.messaging;

import java.util.List;
import java.util.UUID;

import com.shop.product.domain.ProductValidation;

/** Topics, message types and payloads (the {@code data} part of the envelope) of the Product service. */
public final class ProductMessages {

    public static final String COMMANDS_TOPIC = "product.commands";
    public static final String VALIDATE_PRODUCTS = "ValidateProducts";
    public static final String PRODUCTS_VALIDATED = "ProductsValidated";
    public static final String PRODUCTS_REJECTED = "ProductsRejected";

    private ProductMessages() {
    }

    public record ValidateProductsData(List<Item> items) {

        public record Item(UUID productId, int quantity) {
        }
    }

    public record ProductsValidatedData(List<ProductValidation.PricedLine> items) {
    }

    public record ProductsRejectedData(List<ProductValidation.Rejection> rejections) {
    }
}
```

- [ ] **Step 5: Create the handler. It reads the command, loads the catalogue, calls the pure `validate`, then writes one outbox row on the command's `replyTo` topic, keyed by the order id. It runs inside the dispatcher's transaction together with the inbox row**

Create `product-service/src/main/java/com/shop/product/messaging/ValidateProductsHandler.java`:

```java
package com.shop.product.messaging;

import java.util.List;
import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.product.domain.Product;
import com.shop.product.domain.ProductValidation;
import com.shop.product.messaging.ProductMessages.ProductsRejectedData;
import com.shop.product.messaging.ProductMessages.ProductsValidatedData;
import com.shop.product.messaging.ProductMessages.ValidateProductsData;
import com.shop.product.repository.ProductRepository;
import reactor.core.publisher.Mono;

/**
 * Saga step "ValidateProducts": read the catalogue, decide with the pure
 * {@link ProductValidation#validate}, and answer through the outbox on the topic named by the
 * command's {@code replyTo}. Read-only on the catalogue, so a rejection needs no compensation.
 * Runs inside the dispatcher's transaction (inbox row + outbox row commit together).
 */
public class ValidateProductsHandler implements Handler {

    static final String AGGREGATE_TYPE = "Order";

    private final ProductRepository products;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ValidateProductsHandler(ProductRepository products, OutboxWriter outbox, JsonCodec codec) {
        this.products = products;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ValidateProductsData> command = message.envelope(codec, ValidateProductsData.class);
        List<UUID> requested = requestedIds(command);
        String orderId = message.key() != null ? message.key() : command.sagaId().toString();

        return products.findByIds(requested)
                .collectMap(Product::id)
                .map(catalogue -> ProductValidation.validate(requested, catalogue))
                .flatMap(result -> reply(orderId, command, result))
                .then();
    }

    private Mono<?> reply(String orderId, Envelope<ValidateProductsData> command, ProductValidation.Result result) {
        return switch (result) {
            case ProductValidation.Validated ok -> outbox.write(
                    command.replyTo(), AGGREGATE_TYPE, orderId, ProductMessages.PRODUCTS_VALIDATED,
                    command.withData(new ProductsValidatedData(ok.lines())));
            case ProductValidation.Rejected ko -> outbox.write(
                    command.replyTo(), AGGREGATE_TYPE, orderId, ProductMessages.PRODUCTS_REJECTED,
                    command.withData(new ProductsRejectedData(ko.rejections())));
        };
    }

    private static List<UUID> requestedIds(Envelope<ValidateProductsData> command) {
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ValidateProducts without replyTo");
        }
        ValidateProductsData data = command.data();
        if (data == null || data.items() == null || data.items().isEmpty()) {
            throw new NonRetryableException("ValidateProducts without items");
        }
        return data.items().stream().map(ValidateProductsData.Item::productId).toList();
    }
}
```

- [ ] **Step 6: Wire the consumer (registry, dispatcher with consumer name `product-service.product.commands`, single listener). Both classes are conditional on `shop.kafka.bootstrap-servers`**

Create `product-service/src/main/java/com/shop/product/messaging/ProductMessagingConfig.java`:

```java
package com.shop.product.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.outbox.OutboxWriter;
import com.shop.product.repository.ProductRepository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: which handler answers which command type on {@code product.commands}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class ProductMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "product-service." + ProductMessages.COMMANDS_TOPIC;

    @Bean
    public ValidateProductsHandler validateProductsHandler(
            ProductRepository products, OutboxWriter outbox, JsonCodec codec) {
        return new ValidateProductsHandler(products, outbox, codec);
    }

    @Bean
    public HandlerRegistry productCommandHandlers(ValidateProductsHandler validateProducts) {
        return HandlerRegistry.builder()
                .on(ProductMessages.VALIDATE_PRODUCTS, validateProducts)
                .build();
    }

    @Bean
    public MessageDispatcher productCommandsDispatcher(
            HandlerRegistry productCommandHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(
                CONSUMER_NAME, productCommandHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
```

Create `product-service/src/main/java/com/shop/product/messaging/ProductCommandsListener.java`:

```java
package com.shop.product.messaging;

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
public class ProductCommandsListener {

    private final MessageDispatcher dispatcher;

    public ProductCommandsListener(MessageDispatcher productCommandsDispatcher) {
        this.dispatcher = productCommandsDispatcher;
    }

    @KafkaListener(topics = ProductMessages.COMMANDS_TOPIC, containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY)
    void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        dispatcher.onMessage(record, acknowledgment);
    }
}
```

- [ ] **Step 7: Run the whole build**

```bash
cd product-service && mvn -B verify
```

Expected: 10 unit tests, then `ProductApiIT` 10 and `ValidateProductsIT` 5, `Tests run: 15, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 8: Commit**

```bash
git add product-service
git commit -m "feat(product): ValidateProducts saga step with outbox reply"
```

### Task 7: Full loop through Debezium (manual check)

**Files:**

- Modify: none

- [ ] **Step 1: Start the infrastructure (from plan 1)**

```bash
cd infra
docker compose up -d --wait product-db kafka connect
docker compose up kafka-init
```

Expected: services healthy, `kafka-init` exits 0.

- [ ] **Step 2: Start the service in a second terminal. Flyway creates `products` and the outbox/inbox tables in `product_db` (host port 5441)**

```bash
cd product-service && mvn -B spring-boot:run
```

Expected log lines: Flyway `Successfully applied 2 migrations`, then `Started ProductServiceApplication`.

- [ ] **Step 3: Register the product connector (the outbox table now exists)**

```bash
cd infra && ./scripts/register-connectors.sh product
```

Expected: the final JSON lists `product-outbox` with state `RUNNING`.

- [ ] **Step 4: Create a product**

```bash
curl -s -X POST localhost:8081/products -H 'Content-Type: application/json' \
  -d '{"sku":"MUG-1","name":"Mug","price":12.50}'
```

Expected: HTTP 201 and a JSON body; note the `id`.

- [ ] **Step 5: Send a `ValidateProducts` command. There is no Order service yet, so insert it into the product database's own outbox: Debezium publishes it to `product.commands` exactly as Order's connector will. Replace `<PRODUCT_ID>`**

```bash
ORDER_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
cd infra && docker compose exec -T product-db psql -U postgres -d product_db <<SQL
INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
VALUES (gen_random_uuid(), 'product.commands', 'Order', '$ORDER_ID', 'ValidateProducts',
        '{"sagaId":"$ORDER_ID","correlationId":"$ORDER_ID","replyTo":"order.saga.replies","data":{"items":[{"productId":"<PRODUCT_ID>","quantity":2}]}}');
SQL
```

- [ ] **Step 6: Read the reply**

```bash
cd infra && docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic order.saga.replies --from-beginning --max-messages 1 \
  --timeout-ms 30000 --property print.headers=true --property print.key=true
```

Expected: a record with header `type:ProductsValidated`, key = the order id, and a value containing `"unitPrice":12.50`. Repeat with a random product id to see `type:ProductsRejected` and `NOT_FOUND`.

If nothing arrives, check in this order: the service log (handler error or DLT), `curl -s localhost:8083/connectors/product-outbox/status`, and `product.commands.DLT`. This is the first time the loop runs through Debezium.

- [ ] **Step 7: Stop the service (Ctrl+C) and the stack**

```bash
cd infra && docker compose down
```

---

## Done when

- `cd product-service && mvn -B verify` is green (10 unit + 15 integration tests).
- The Debezium loop of Task 7 returns `ProductsValidated` for an existing product.

Then continue with plan 3 (Inventory service), which follows the same structure (migration `V2`, repository, handlers `ReserveStock` and `ReleaseStock`, bucketed stock).
