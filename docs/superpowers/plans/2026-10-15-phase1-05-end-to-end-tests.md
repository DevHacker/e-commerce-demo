# Phase 1, plan 5: End-to-end tests

> **Update:** the stack now has five services and four connectors (Payment, Customer added) and the scenarios cover payment (declined card, every method, refund on cancel, payment down). The current code is in `e2e-tests/`; see `docs/superpowers/specs/2026-10-15-phase2-payment-customer-design.md`.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the whole system works together: Product, Inventory and Order running as real processes, talking only through Kafka and Debezium, in the scenarios of spec section 9.

**Architecture:** A new Maven module `e2e-tests` with no production code. `Stack` starts the compose infrastructure (`infra/docker-compose.yml`: three PostgreSQL 18, Kafka, Debezium Connect), then the three services from their built jars, then registers the three outbox connectors from `infra/debezium/*.json` and waits for them to be `RUNNING`. Tests drive the public HTTP APIs only. One stack per JVM, shared by all test classes.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Awaitility, `java.net.http`, Jackson 3, `kafka-clients` (replay), Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-15-ecommerce-phase1-design.md` (section 9, end-to-end scenarios)

**Prerequisite:** plans 1 to 4 done and merged; Docker running; `docker compose` on the PATH; ports 5441, 5442, 5443, 29092, 8080 to 8083 free (stop the development stack first).

## Verification status (read before starting)

The module **compiles** (`mvn test-compile`). It has **never been run**: the authoring sandbox had no Docker daemon. Expect the first run to need small fixes, most likely in `Stack` (timings, compose flags) or in the Debezium connector settings. That is the point of this plan: it is the first time Debezium sits in the loop. The mechanics the tests rely on, each already covered by a faster test: saga logic by `SagaFlowIT`, stock allocation by `ConcurrentReservationIT`, handlers and inbox by `StockCommandsIT` / `ValidateProductsIT`.

## Decisions

1. **Jars, not in-process contexts.** The three services share package roots and Flyway locations (`db/migration`), so they cannot live in one classpath. Each runs as its own `java -jar` process, with logs in `e2e-tests/target/e2e-logs/`.
2. **The compose file is the test infrastructure.** No second description of the stack to keep in sync. Cost: fixed host ports.
3. **Short saga timeout for Order only** (`SHOP_ORDER_SAGA_TIMEOUT=20s`, sweep 1 s), so the timeout scenario takes about 25 s. Normal scenarios finish in a few seconds, well under 20 s.
4. **Assertions on public APIs.** State is read through `GET /orders`, `/orders/{id}/history`, `/stock/{id}`. The one exception is `Replay`, which republishes a reply on Kafka to simulate at-least-once delivery.
5. **Concurrency assertion is "never oversell", not "exactly 20 confirmed".** A reservation that sees a bucket another order has temporarily taken from can be rejected although stock remains after the other order backs off. The test asserts `reserved == confirmed * quantity`, `confirmed <= 20`, no bucket over-reserved, every saga terminal.
6. **Scenarios** (spec 9): nominal; rejected product (inactive, unknown); rejected stock; cancellation with compensation; redelivery (same bytes and headers, plus a late reply, both without effect); concurrent stock on a 4-bucket product; Product down, saga timeout, late answer ignored.

## File structure

```
e2e-tests/
  pom.xml
  src/test/java/com/shop/e2e/
    Stack            compose + 3 jars + connectors, once per JVM
    Api              HTTP helpers over the 3 services
    Replay           republish a delivered reply
    OrderFlowIT      nominal, rejected product x2, rejected stock, cancel, redelivery
    ConcurrentStockIT
    SagaTimeoutIT
```

---

### Task 1: Module, stack and helpers

**Files:**
- Create: `e2e-tests/pom.xml`
- Create: `e2e-tests/src/test/java/com/shop/e2e/{Stack,Api,Replay}.java`

- [ ] **Step 1: Create the POM (test-scoped dependencies only; failsafe runs `*IT`)**

`e2e-tests/pom.xml`

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
    <artifactId>e2e-tests</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>e2e-tests</name>
    <description>End-to-end tests: the three services as jars, docker compose infrastructure, Debezium in the loop.</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>tools.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- No unit tests here. End-to-end tests are *IT (failsafe) and need Docker and the three service jars. -->
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

- [ ] **Step 2: Create the stack launcher**

`e2e-tests/src/test/java/com/shop/e2e/Stack.java`

```java
package com.shop.e2e;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The whole system, started once per JVM: infrastructure from {@code infra/docker-compose.yml}
 * (three PostgreSQL, Kafka, Debezium Connect), the three services as the jars Maven built, and the three
 * outbox connectors. Uses the fixed host ports of the compose file, so the development stack must be down.
 */
public final class Stack {

    public static final String PRODUCT = "http://localhost:8081";
    public static final String ORDER = "http://localhost:8080";
    public static final String INVENTORY = "http://localhost:8082";
    public static final String CONNECT = "http://localhost:8083";
    public static final String KAFKA = "localhost:29092";

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final Path COMPOSE = ROOT.resolve("infra/docker-compose.yml");
    private static final Path LOGS = Path.of("target/e2e-logs").toAbsolutePath();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static final Map<String, Process> PROCESSES = new LinkedHashMap<>();
    private static boolean started;

    private Stack() {
    }

    /** Idempotent: the first test class starts everything, the others reuse it. */
    public static synchronized void start() {
        if (started) {
            return;
        }
        try {
            Files.createDirectories(LOGS);
            Runtime.getRuntime().addShutdownHook(new Thread(Stack::stop, "e2e-stack-stop"));
            compose("up", "-d", "--wait", "product-db", "order-db", "inventory-db", "kafka", "connect");
            compose("up", "kafka-init");
            startService("product", 8081, Map.of());
            startService("inventory", 8082, Map.of());
            // short saga timeout so the timeout scenario does not take minutes; long enough for a cold start
            startService("order", 8080, Map.of("SHOP_ORDER_SAGA_TIMEOUT", "20s", "SHOP_ORDER_SWEEP_INTERVAL", "1s"));
            for (String name : List.of("product", "order", "inventory")) {
                registerConnector(name);
            }
            started = true;
        } catch (Exception e) {
            stop();
            throw new IllegalStateException("Could not start the end-to-end stack: " + e.getMessage(), e);
        }
    }

    public static synchronized void stopProduct() {
        Process p = PROCESSES.remove("product");
        if (p != null) {
            p.destroy();
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static synchronized void startProduct() {
        Process running = PROCESSES.get("product");
        if (running != null && running.isAlive()) {
            return;
        }
        try {
            startService("product", 8081, Map.of());
        } catch (Exception e) {
            throw new IllegalStateException("Could not restart product-service: " + e.getMessage(), e);
        }
    }

    private static synchronized void stop() {
        PROCESSES.values().forEach(Process::destroy);
        PROCESSES.clear();
        if (!Boolean.getBoolean("e2e.keep")) {
            try {
                compose("down", "-v");
            } catch (Exception ignored) {
                // best effort at shutdown
            }
        }
    }

    // ---- processes -------------------------------------------------------------------------------------

    private static void compose(String... args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("docker", "compose", "-f", COMPOSE.toString()));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd)
                .directory(COMPOSE.getParent().toFile())
                .redirectErrorStream(true)
                .redirectOutput(LOGS.resolve("compose.log").toFile())
                .start();
        if (p.waitFor() != 0) {
            throw new IllegalStateException("docker compose " + String.join(" ", args) + " failed, see " + LOGS.resolve("compose.log"));
        }
    }

    private static void startService(String name, int port, Map<String, String> env) throws Exception {
        File jar = ROOT.resolve(name + "-service/target/" + name + "-service-1.0.0-SNAPSHOT.jar").toFile();
        if (!jar.isFile()) {
            throw new IllegalStateException(jar + " not found: run `mvn -DskipTests package` in " + name + "-service first");
        }
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", jar.getPath())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(LOGS.resolve(name + ".log").toFile()));
        pb.environment().putAll(env);
        PROCESSES.put(name, pb.start());
        awaitHealthy(name, port);
    }

    private static void awaitHealthy(String name, int port) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (System.nanoTime() < deadline) {
            if (!PROCESSES.get(name).isAlive()) {
                throw new IllegalStateException(name + "-service exited, see " + LOGS.resolve(name + ".log"));
            }
            try {
                HttpResponse<String> r = get("http://localhost:" + port + "/actuator/health");
                if (r.statusCode() == 200 && r.body().contains("\"UP\"")) {
                    return;
                }
            } catch (IOException notYet) {
                // still starting
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(name + "-service not healthy after 120s, see " + LOGS.resolve(name + ".log"));
    }

    // ---- Debezium --------------------------------------------------------------------------------------

    private static void registerConnector(String name) throws Exception {
        String connector = name + "-outbox";
        String body = Files.readString(ROOT.resolve("infra/debezium/" + connector + ".json"));
        HttpResponse<String> put = HTTP.send(HttpRequest.newBuilder(URI.create(CONNECT + "/connectors/" + connector + "/config"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        if (put.statusCode() / 100 != 2) {
            throw new IllegalStateException("Registering " + connector + " failed: " + put.statusCode() + " " + put.body());
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            HttpResponse<String> status = get(CONNECT + "/connectors/" + connector + "/status");
            if (status.statusCode() == 200) {
                JsonNode node = JSON.readTree(status.body());
                boolean connectorUp = "RUNNING".equals(node.path("connector").path("state").asString());
                boolean taskUp = node.path("tasks").size() > 0 && "RUNNING".equals(node.path("tasks").get(0).path("state").asString());
                if (connectorUp && taskUp) {
                    return;
                }
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(connector + " is not RUNNING after 90s: " + get(CONNECT + "/connectors/" + connector + "/status").body());
    }

    private static HttpResponse<String> get(String url) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
```

- [ ] **Step 3: Create the API helpers**

`e2e-tests/src/test/java/com/shop/e2e/Api.java`

```java
package com.shop.e2e;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** HTTP helpers over the three services, as a client of the public APIs only. */
public final class Api {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private Api() {
    }

    // ---- Product ---------------------------------------------------------------------------------------

    public static UUID createProduct(String price) {
        String sku = "E2E-" + UUID.randomUUID();
        Reply r = send("POST", Stack.PRODUCT + "/products", null,
                "{\"sku\":\"%s\",\"name\":\"Mug %s\",\"price\":%s}".formatted(sku, sku, price));
        require(201, r);
        return UUID.fromString(r.json().get("id").asString());
    }

    public static void deactivate(UUID productId) {
        JsonNode product = send("GET", Stack.PRODUCT + "/products/" + productId, null, null).json();
        Reply r = send("PUT", Stack.PRODUCT + "/products/" + productId, null,
                "{\"name\":\"%s\",\"price\":%s,\"active\":false,\"version\":%d}".formatted(
                        product.get("name").asString(), product.get("price").decimalValue().toPlainString(),
                        product.get("version").asLong()));
        require(200, r);
    }

    // ---- Inventory -------------------------------------------------------------------------------------

    public static void setStock(UUID productId, long quantity, int bucketCount) {
        require(200, send("PUT", Stack.INVENTORY + "/stock/" + productId, null,
                "{\"quantity\":%d,\"bucketCount\":%d}".formatted(quantity, bucketCount)));
    }

    public static JsonNode stock(UUID productId) {
        Reply r = send("GET", Stack.INVENTORY + "/stock/" + productId, null, null);
        require(200, r);
        return r.json();
    }

    // ---- Order -----------------------------------------------------------------------------------------

    public static UUID placeOrder(UUID productId, int quantity) {
        String body = "{\"customerRef\":\"e2e\",\"items\":[{\"productId\":\"%s\",\"quantity\":%d}]}".formatted(productId, quantity);
        Reply r = send("POST", Stack.ORDER + "/orders", UUID.randomUUID().toString(), body);
        require(202, r);
        return UUID.fromString(r.json().get("orderId").asString());
    }

    public static JsonNode order(UUID orderId) {
        Reply r = send("GET", Stack.ORDER + "/orders/" + orderId, null, null);
        require(200, r);
        return r.json();
    }

    public static JsonNode history(UUID orderId) {
        Reply r = send("GET", Stack.ORDER + "/orders/" + orderId + "/history", null, null);
        require(200, r);
        return r.json();
    }

    public static int cancel(UUID orderId) {
        return send("POST", Stack.ORDER + "/orders/" + orderId + "/cancel", null, null).status();
    }

    public static BigDecimal total(UUID orderId) {
        return order(orderId).get("total").decimalValue();
    }

    /** Waits until the order has the given status. The whole chain runs through Debezium, hence the generous limit. */
    public static void awaitStatus(UUID orderId, String status) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> org.assertj.core.api.Assertions.assertThat(order(orderId).get("status").asString()).isEqualTo(status));
    }

    /** Waits until the order is no longer PENDING or CANCELLING, whichever way it ended. */
    public static String awaitFinal(UUID orderId, Duration limit) {
        String[] last = new String[1];
        await().atMost(limit).pollInterval(Duration.ofMillis(250)).until(() -> {
            last[0] = order(orderId).get("status").asString();
            return !last[0].equals("PENDING") && !last[0].equals("CANCELLING");
        });
        return last[0];
    }

    /** The reason of the most recent status change. */
    public static String lastReason(UUID orderId) {
        JsonNode status = history(orderId).get("status");
        return status.get(status.size() - 1).path("reason").asString();
    }

    // ---- plumbing --------------------------------------------------------------------------------------

    private record Reply(int status, String body) {
        JsonNode json() {
            return JSON.readTree(body);
        }
    }

    private static Reply send(String method, String url, String idempotencyKey, String body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json");
        if (idempotencyKey != null) {
            b.header("Idempotency-Key", idempotencyKey);
        }
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        try {
            HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            return new Reply(r.statusCode(), r.body());
        } catch (IOException e) {
            throw new IllegalStateException(method + " " + url + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void require(int expected, Reply r) {
        if (r.status() != expected) {
            throw new AssertionError("expected HTTP " + expected + " but got " + r.status() + ": " + r.body());
        }
    }
}
```

- [ ] **Step 4: Create the replay helper**

`e2e-tests/src/test/java/com/shop/e2e/Replay.java`

```java
package com.shop.e2e;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Plays at-least-once delivery: publishes a reply Debezium already delivered once, byte for byte, with its headers. */
public final class Replay {

    private Replay() {
    }

    /** Republishes the first record of {@code topic} for {@code key} whose {@code type} header matches. Returns false if none. */
    public static boolean republish(String topic, String key, String type) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Stack.KAFKA);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-replay-" + UUID.randomUUID());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(500))) {
                    var header = rec.headers().lastHeader("type");
                    if (key.equals(rec.key()) && header != null && type.equals(new String(header.value(), StandardCharsets.UTF_8))) {
                        resend(rec);
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static void resend(ConsumerRecord<String, String> rec) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Stack.KAFKA);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
            producer.send(new ProducerRecord<>(rec.topic(), null, rec.key(), rec.value(), rec.headers())).get();
        } catch (Exception e) {
            throw new IllegalStateException("republish failed: " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 5: Compile**

Run: `cd e2e-tests && mvn -B -q test-compile`

Expected: no output, exit code 0.

- [ ] **Step 6: Commit**

```bash
git add e2e-tests
git commit -m "test(e2e): stack launcher and API helpers"
```


### Task 2: Order flow scenarios

**Files:**
- Create: `e2e-tests/src/test/java/com/shop/e2e/OrderFlowIT.java`

- [ ] **Step 1: Write the scenarios**

`e2e-tests/src/test/java/com/shop/e2e/OrderFlowIT.java`

```java
package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;

/** The saga across the three real services, with Debezium carrying every message. */
class OrderFlowIT {

    @BeforeAll
    static void stack() {
        Stack.start();
    }

    @Test
    void nominalFlowConfirmsTheOrderAndReservesTheStock() {
        UUID product = Api.createProduct("12.50");
        Api.setStock(product, 10, 2);

        UUID order = Api.placeOrder(product, 2);
        Api.awaitStatus(order, "CONFIRMED");

        assertThat(Api.total(order)).isEqualByComparingTo(new BigDecimal("25.00"));
        JsonNode stock = Api.stock(product);
        assertThat(stock.get("reserved").asLong()).isEqualTo(2);
        assertThat(stock.get("free").asLong()).isEqualTo(8);
        JsonNode status = Api.history(order).get("status");
        assertThat(status.size()).isEqualTo(2);
        assertThat(status.get(1).get("to").asString()).isEqualTo("CONFIRMED");
    }

    @Test
    void inactiveProductRejectsTheOrderAndTouchesNoStock() {
        UUID product = Api.createProduct("5.00");
        Api.setStock(product, 10, 1);
        Api.deactivate(product);

        UUID order = Api.placeOrder(product, 1);
        Api.awaitStatus(order, "REJECTED");

        assertThat(Api.lastReason(order)).isEqualTo("PRODUCT_REJECTED");
        assertThat(Api.stock(product).get("reserved").asLong()).isZero();
    }

    @Test
    void unknownProductRejectsTheOrder() {
        UUID order = Api.placeOrder(UUID.randomUUID(), 1);
        Api.awaitStatus(order, "REJECTED");
        assertThat(Api.lastReason(order)).isEqualTo("PRODUCT_REJECTED");
    }

    @Test
    void insufficientStockRejectsTheOrderAndReservesNothing() {
        UUID product = Api.createProduct("3.00");
        Api.setStock(product, 3, 1);

        UUID order = Api.placeOrder(product, 5);
        Api.awaitStatus(order, "REJECTED");

        assertThat(Api.lastReason(order)).isEqualTo("STOCK_REJECTED");
        JsonNode stock = Api.stock(product);
        assertThat(stock.get("reserved").asLong()).isZero();
        assertThat(stock.get("onHand").asLong()).isEqualTo(3);
    }

    @Test
    void cancellingAConfirmedOrderGivesTheStockBack() {
        UUID product = Api.createProduct("8.00");
        Api.setStock(product, 10, 2);
        UUID order = Api.placeOrder(product, 4);
        Api.awaitStatus(order, "CONFIRMED");
        assertThat(Api.stock(product).get("reserved").asLong()).isEqualTo(4);

        assertThat(Api.cancel(order)).isEqualTo(202);
        Api.awaitStatus(order, "CANCELLED");

        JsonNode stock = Api.stock(product);
        assertThat(stock.get("reserved").asLong()).isZero();
        assertThat(stock.get("free").asLong()).isEqualTo(10);
        assertThat(Api.cancel(order)).isEqualTo(409);
    }

    @Test
    void aRedeliveredReplyHasNoSecondEffect() throws Exception {
        UUID product = Api.createProduct("2.00");
        Api.setStock(product, 10, 1);
        UUID order = Api.placeOrder(product, 3);
        Api.awaitStatus(order, "CONFIRMED");
        long version = Api.order(order).get("version").asLong();
        int statusEntries = Api.history(order).get("status").size();

        assertThat(Replay.republish("order.saga.replies", order.toString(), "StockReserved")).isTrue();
        assertThat(Replay.republish("order.saga.replies", order.toString(), "ProductsValidated")).isTrue();
        Thread.sleep(5_000);

        assertThat(Api.order(order).get("status").asString()).isEqualTo("CONFIRMED");
        assertThat(Api.order(order).get("version").asLong()).isEqualTo(version);
        assertThat(Api.history(order).get("status").size()).isEqualTo(statusEntries);
        assertThat(Api.stock(product).get("reserved").asLong()).isEqualTo(3);
    }
}
```

- [ ] **Step 2: Build the three jars**

```bash
for s in product inventory order; do (cd $s-service && mvn -B -q -DskipTests package); done
```

Expected: `product-service/target/product-service-1.0.0-SNAPSHOT.jar` and the two others exist.

- [ ] **Step 3: Run**

```bash
cd e2e-tests && mvn -B verify -Dit.test=OrderFlowIT
```

Expected: `Tests run: 6, Failures: 0, Errors: 0`. The first run pulls images and takes a few minutes. When something stalls, read in this order: `target/e2e-logs/*.log`, `curl -s localhost:8083/connectors?expand=status`, the topics (`docker compose -f ../infra/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic order.saga.replies.DLT --from-beginning --timeout-ms 5000`). Add `-De2e.keep=true` to leave the stack up after the run.

- [ ] **Step 4: Commit**

```bash
git add e2e-tests
git commit -m "test(e2e): order flow scenarios"
```


### Task 3: Concurrent stock and saga timeout

**Files:**
- Create: `e2e-tests/src/test/java/com/shop/e2e/{ConcurrentStockIT,SagaTimeoutIT}.java`

- [ ] **Step 1: Write both scenarios**

`e2e-tests/src/test/java/com/shop/e2e/ConcurrentStockIT.java`

```java
package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;

/** Many orders racing for one product spread over several stock buckets: never oversold, all sagas end. */
class ConcurrentStockIT {

    @BeforeAll
    static void stack() {
        Stack.start();
    }

    @Test
    void ordersRacingForAMultiBucketProductNeverOversell() throws Exception {
        UUID product = Api.createProduct("1.00");
        Api.setStock(product, 100, 4);
        int orders = 30;
        int quantity = 5; // 150 requested for 100 in stock

        List<UUID> ids = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(15);
        try {
            List<Future<UUID>> futures = new ArrayList<>();
            for (int i = 0; i < orders; i++) {
                futures.add(pool.submit(() -> Api.placeOrder(product, quantity)));
            }
            for (Future<UUID> f : futures) {
                ids.add(f.get());
            }
        } finally {
            pool.shutdown();
        }

        int confirmed = 0;
        for (UUID id : ids) {
            String end = Api.awaitFinal(id, Duration.ofSeconds(120));
            assertThat(end).isIn("CONFIRMED", "REJECTED");
            if (end.equals("CONFIRMED")) {
                confirmed++;
            }
        }

        JsonNode stock = Api.stock(product);
        assertThat(confirmed).isBetween(1, 20);
        assertThat(stock.get("reserved").asLong()).isEqualTo(confirmed * (long) quantity);
        assertThat(stock.get("free").asLong()).isGreaterThanOrEqualTo(0);
        for (JsonNode bucket : stock.get("buckets")) {
            assertThat(bucket.get("reserved").asLong()).isBetween(0L, bucket.get("onHand").asLong());
        }
    }
}
```

`e2e-tests/src/test/java/com/shop/e2e/SagaTimeoutIT.java`

```java
package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Product is down: the order service gives up on the saga after its timeout (20 s here) and rejects the order. */
class SagaTimeoutIT {

    @BeforeAll
    static void stack() {
        Stack.start();
    }

    @AfterAll
    static void bringProductBack() {
        Stack.startProduct();
    }

    @Test
    void silenceFromProductRejectsTheOrderAndALateAnswerChangesNothing() throws Exception {
        UUID product = Api.createProduct("4.00");
        Api.setStock(product, 10, 1);
        Stack.stopProduct();

        UUID order = Api.placeOrder(product, 1);
        assertThat(Api.awaitFinal(order, Duration.ofSeconds(90))).isEqualTo("REJECTED");
        assertThat(Api.lastReason(order)).isEqualTo("TIMEOUT");

        // Product comes back, finds the old command, answers: the reply is late and must be ignored.
        Stack.startProduct();
        Thread.sleep(8_000);
        assertThat(Api.order(order).get("status").asString()).isEqualTo("REJECTED");
        assertThat(Api.stock(product).get("reserved").asLong()).isZero();
    }
}
```

- [ ] **Step 2: Run everything**

```bash
cd e2e-tests && mvn -B verify
```

Expected: `Tests run: 8, Failures: 0, Errors: 0` (6 + 1 + 1), `BUILD SUCCESS`, and the compose stack removed at the end.

- [ ] **Step 3: Commit**

```bash
git add e2e-tests
git commit -m "test(e2e): concurrent stock and saga timeout"
```


---

## Done when

- `mvn -B verify` in `e2e-tests` is green (8 scenarios) on a machine with Docker.
- Any fix needed to get there is committed with the reason in its message.

Phase 1 is then complete. Phase 3 (Payment) and phase 4 (Customer) follow.
