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
