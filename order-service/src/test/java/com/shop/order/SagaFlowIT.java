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
        awaitOutbox(orderId, "ProcessPayment");
        send(orderId, "PaymentCompleted", reply(orderId, "{\"paymentId\":\"p1\",\"status\":\"CAPTURED\"}"));
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

        // Payment is asked to charge the snapshot total with the method chosen at creation
        Map<String, Object> charge = awaitOutbox(orderId, "ProcessPayment");
        assertThat(charge.get("topic")).isEqualTo("payment.commands");
        JsonNode chargeData = payload(charge).get("data");
        assertThat(chargeData.get("amount").decimalValue()).isEqualByComparingTo("27.00"); // 2 x 10.50 + 2 x 3.00
        assertThat(chargeData.get("method").asString()).isEqualTo("STRIPE");
        assertThat(chargeData.get("details").get("paymentMethodId").asString()).isEqualTo("pm_card_visa");
        assertThat(statusOf(orderId)).isEqualTo("PENDING");

        send(orderId, "PaymentCompleted", reply(orderId, "{\"paymentId\":\"p1\",\"status\":\"CAPTURED\"}"));
        awaitStatus(orderId, "CONFIRMED");

        JsonNode order = order(orderId);
        assertThat(order.get("paymentMethod").asString()).isEqualTo("STRIPE");
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
                .containsExactly("ValidateProducts:REQUESTED", "ValidateProducts:VALIDATED", "ReserveStock:RESERVED",
                        "ProcessPayment:CAPTURED");
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
    void cancellingAConfirmedOrderRefundsThenReleasesTheStockThenCancels() throws Exception {
        UUID orderId = confirmedOrder();

        assertThat(cancel(orderId)).isEqualTo(HttpStatus.ACCEPTED);

        // 1. the money goes back
        assertThat(statusOf(orderId)).isEqualTo("CANCELLING");
        Map<String, Object> refund = awaitOutbox(orderId, "RefundPayment");
        assertThat(refund.get("topic")).isEqualTo("payment.commands");
        assertThat(payload(refund).get("data").get("reason").asString()).isEqualTo("ORDER_CANCELLED");
        assertThat(outbox(orderId, "ReleaseStock")).as("stock is released only after the refund").isEmpty();
        assertThat(cancel(orderId)).as("a second cancel while CANCELLING").isEqualTo(HttpStatus.CONFLICT);

        // 2. then the stock
        send(orderId, "PaymentRefunded", reply(orderId, "{\"refunded\":true}"));
        Map<String, Object> release = awaitOutbox(orderId, "ReleaseStock");
        assertThat(release.get("topic")).isEqualTo("inventory.commands");
        assertThat(payload(release).get("data").get("reason").asString()).isEqualTo("PAYMENT_REFUNDED");

        send(orderId, "StockReleased", reply(orderId, "{\"released\":[]}"));
        awaitStatus(orderId, "CANCELLED");

        awaitOutbox(orderId, "OrderCancelled");
        assertThat(history(orderId).get("steps"))
                .extracting(n -> n.get("direction").asString() + ":" + n.get("step").asString() + ":" + n.get("outcome").asString())
                .containsSubsequence("COMPENSATE:RefundPayment:REQUESTED", "COMPENSATE:RefundPayment:REFUNDED",
                        "COMPENSATE:ReleaseStock:REQUESTED", "COMPENSATE:ReleaseStock:RELEASED");
        assertThat(history(orderId).get("status")).extracting(n -> n.get("to").asString())
                .containsExactly("PENDING", "CONFIRMED", "CANCELLING", "CANCELLED");
    }

    @Test
    void aFailedPaymentGivesTheStockBackAndRejectsTheOrder() throws Exception {
        UUID orderId = createOrder(a, b);
        awaitOutbox(orderId, "ValidateProducts");
        send(orderId, "ProductsValidated", productsValidated(orderId, prices()));
        awaitOutbox(orderId, "ReserveStock");
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));
        awaitOutbox(orderId, "ProcessPayment");

        send(orderId, "PaymentFailed", reply(orderId, "{\"code\":\"CARD_DECLINED\",\"message\":\"The card was declined\"}"));

        Map<String, Object> release = awaitOutbox(orderId, "ReleaseStock");
        assertThat(payload(release).get("data").get("reason").asString()).isEqualTo("PAYMENT_FAILED");
        assertThat(outbox(orderId, "RefundPayment")).as("nothing was charged").isEmpty();
        assertThat(statusOf(orderId)).as("not rejected until the stock is back").isEqualTo("PENDING");

        send(orderId, "StockReleased", reply(orderId, "{\"released\":[]}"));
        awaitStatus(orderId, "REJECTED");

        awaitOutbox(orderId, "OrderRejected");
        assertThat(history(orderId).get("status")).extracting(n -> n.get("reason").asString())
                .containsExactly("ORDER_CREATED", "PAYMENT_FAILED");
        JsonNode steps = history(orderId).get("steps");
        assertThat(steps).extracting(n -> n.get("step").asString() + ":" + n.get("outcome").asString())
                .contains("ProcessPayment:FAILED", "ReleaseStock:RELEASED");
        assertThat(steps).filteredOn(n -> "FAILED".equals(n.get("outcome").asString()))
                .singleElement().satisfies(n -> assertThat(n.get("detail").get("code").asString()).isEqualTo("CARD_DECLINED"));
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
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}")); // duplicate: ignored
        send(orderId, "PaymentCompleted", reply(orderId, "{\"status\":\"CAPTURED\"}"));
        send(orderId, "PaymentCompleted", reply(orderId, "{\"status\":\"CAPTURED\"}")); // duplicate: ignored

        awaitStatus(orderId, "CONFIRMED"); // same key = same partition: the duplicates were consumed before this
        send(orderId, "StockReserved", reply(orderId, "{\"reservations\":[]}"));
        send(orderId, "PaymentFailed", reply(orderId, "{\"code\":\"LATE\",\"message\":\"late\"}")); // late: ignored
        UUID marker = createOrder(a, b);
        awaitOutbox(marker, "ValidateProducts");
        send(marker, "ProductsValidated", productsValidated(marker, prices()));
        awaitOutbox(marker, "ReserveStock"); // the late replies above were consumed before this one

        assertThat(statusOf(orderId)).isEqualTo("CONFIRMED");
        assertThat(outbox(orderId, "ReserveStock")).hasSize(1);
        assertThat(outbox(orderId, "ProcessPayment")).hasSize(1);
        assertThat(outbox(orderId, "ReleaseStock")).isEmpty();
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
