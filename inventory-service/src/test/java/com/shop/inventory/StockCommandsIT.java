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
