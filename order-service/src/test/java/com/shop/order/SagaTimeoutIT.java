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
        assertThat(outbox(orderId, "RefundPayment")).as("no payment was ever requested").isEmpty();
    }

    @Test
    void noAnswerFromPaymentRefundsFirstThenReleasesTheStockAndCancels() throws Exception {
        UUID orderId = createOrder(a);
        awaitOutbox(orderId, "ValidateProducts");
        send(orderId, "ProductsValidated", productsValidated(orderId, Map.of(a, "5.00")));
        awaitOutbox(orderId, "ReserveStock");
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));
        awaitOutbox(orderId, "ProcessPayment");

        // Payment stays silent: the money may have been taken, so the sweeper asks for a refund first
        awaitStatus(orderId, "CANCELLING");
        Map<String, Object> refund = awaitOutbox(orderId, "RefundPayment");
        assertThat(payload(refund).get("data").get("reason").asString()).isEqualTo("TIMEOUT");
        assertThat(outbox(orderId, "ReleaseStock")).isEmpty();

        send(orderId, "PaymentRefunded", reply(orderId, "{\"refunded\":false}"));
        awaitOutbox(orderId, "ReleaseStock");
        send(orderId, "StockReleased", reply(orderId, "{\"released\":[]}"));
        awaitStatus(orderId, "CANCELLED");
        assertThat(history(orderId).get("steps"))
                .extracting(n -> n.get("step").asString() + ":" + n.get("outcome").asString())
                .containsSubsequence("ProcessPayment:TIMED_OUT", "RefundPayment:NOTHING_TO_REFUND", "ReleaseStock:RELEASED");
    }
}
