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
