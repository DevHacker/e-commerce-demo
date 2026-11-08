package com.shop.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.payment.support.KafkaSupport;
import com.shop.payment.support.KafkaTestClient;
import com.shop.payment.support.PostgresSupport;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Real PostgreSQL + real Kafka, no Debezium: the test publishes like Debezium would (headers {@code id}
 * and {@code type}) and reads the replies from the {@code outbox_events} table the handlers wrote.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class PaymentCommandsIT {

    private static final String COMMANDS = "payment.commands";
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
        registry.add("shop.kafka.group-id", () -> "payment-it-" + UUID.randomUUID());
        registry.add("shop.kafka.concurrency", () -> "1");
        registry.add("shop.kafka.retry.initial-interval", () -> "50ms");
        registry.add("shop.kafka.retry.max-interval", () -> "100ms");
        registry.add("shop.kafka.retry.max-attempts", () -> "2");
    }

    @Autowired
    DatabaseClient db;

    @Autowired
    JsonMapper json;

    @Autowired
    WebTestClient client;

    @BeforeEach
    void clean() {
        db.sql("TRUNCATE payments, outbox_events, processed_events").fetch().rowsUpdated().block();
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    private static String command(UUID orderId, String data) {
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"%s\",\"data\":%s}"
                .formatted(orderId, orderId, REPLIES, data);
    }

    private static String charge(String amount, String method, String detailsJson) {
        return "{\"amount\":%s,\"method\":\"%s\",\"details\":%s}".formatted(amount, method, detailsJson);
    }

    private void processPayment(UUID orderId, UUID eventId, String data) throws Exception {
        KAFKA.send(COMMANDS, orderId.toString(), eventId, "ProcessPayment", command(orderId, data));
    }

    private void processPayment(UUID orderId, String data) throws Exception {
        processPayment(orderId, UUID.randomUUID(), data);
    }

    private void refund(UUID orderId) throws Exception {
        KAFKA.send(COMMANDS, orderId.toString(), UUID.randomUUID(), "RefundPayment", command(orderId, "{\"reason\":\"ORDER_CANCELLED\"}"));
    }

    private List<Map<String, Object>> outboxRows() {
        return db.sql("SELECT topic, aggregatetype, aggregateid, type, payload::text AS payload "
                        + "FROM outbox_events ORDER BY occurred_at, id")
                .fetch().all().collectList().block();
    }

    private List<Map<String, Object>> replies(UUID orderId, String type) {
        return outboxRows().stream()
                .filter(r -> orderId.toString().equals(r.get("aggregateid")) && type.equals(r.get("type"))).toList();
    }

    private Map<String, Object> awaitReply(UUID orderId, String type) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> !replies(orderId, type).isEmpty());
        return replies(orderId, type).get(0);
    }

    private JsonNode data(Map<String, Object> row) {
        return json.readTree((String) row.get("payload")).get("data");
    }

    private JsonNode paymentOf(UUID orderId) {
        String body = client.get().uri("/payments/{orderId}", orderId).exchange()
                .expectStatus().isOk().returnResult(String.class).getResponseBody().blockFirst();
        return json.readTree(body);
    }

    // ---- one test per method: the right strategy is chosen --------------------------------------------

    static List<Arguments> methodsThatCharge() {
        return List.of(
                Arguments.of("CREDIT_CARD", "{\"cardToken\":\"tok_visa\"}", "CAPTURED", "cc_"),
                Arguments.of("credit_card", "{\"cardToken\":\"tok_visa\"}", "CAPTURED", "cc_"),
                Arguments.of("CASH_ON_DELIVERY", "{\"deliveryAddress\":\"12 rue des Lilas\"}", "PENDING_ON_DELIVERY", "cod_"),
                Arguments.of("PAYPAL", "{\"payerEmail\":\"fidele@example.com\"}", "CAPTURED", "pp_"),
                Arguments.of("RAZORPAY", "{\"razorpayPaymentId\":\"pay_29QQoUBi66xm2f\",\"razorpaySignature\":\"abc\"}", "CAPTURED", "pay_"),
                Arguments.of("STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}", "CAPTURED", "ch_"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("methodsThatCharge")
    void eachMethodIsHandledByItsOwnStrategy(String method, String details, String status, String referencePrefix) throws Exception {
        UUID order = UUID.randomUUID();

        processPayment(order, charge("42.50", method, details));

        Map<String, Object> reply = awaitReply(order, "PaymentCompleted");
        assertThat(reply.get("topic")).isEqualTo(REPLIES);
        assertThat(reply.get("aggregatetype")).isEqualTo("Order");
        JsonNode data = data(reply);
        assertThat(data.get("status").asString()).isEqualTo(status);
        assertThat(data.get("method").asString()).isEqualTo(method.toUpperCase());
        assertThat(data.get("reference").asString()).startsWith(referencePrefix);

        JsonNode payment = paymentOf(order);
        assertThat(payment.get("status").asString()).isEqualTo(status);
        assertThat(payment.get("amount").decimalValue()).isEqualByComparingTo("42.50");
        assertThat(payment.get("paymentId").asString()).isEqualTo(data.get("paymentId").asString());
    }

    // ---- failures -----------------------------------------------------------------------------------------------

    static List<Arguments> failures() {
        return List.of(
                Arguments.of("declined card", "10.00", "CREDIT_CARD", "{\"cardToken\":\"tok_declined\"}", "CARD_DECLINED"),
                Arguments.of("no funds", "10.00", "CREDIT_CARD", "{\"cardToken\":\"tok_insufficient_funds\"}", "INSUFFICIENT_FUNDS"),
                Arguments.of("stripe declined", "10.00", "STRIPE", "{\"paymentMethodId\":\"pm_card_chargeDeclined\"}", "CARD_DECLINED"),
                Arguments.of("paypal declined", "10.00", "PAYPAL", "{\"payerEmail\":\"declined@example.com\"}", "PAYER_DECLINED"),
                Arguments.of("bad signature", "10.00", "RAZORPAY", "{\"razorpayPaymentId\":\"pay_1\",\"razorpaySignature\":\"invalid\"}", "SIGNATURE_MISMATCH"),
                Arguments.of("cod over limit", "500.01", "CASH_ON_DELIVERY", "{\"deliveryAddress\":\"somewhere\"}", "COD_LIMIT_EXCEEDED"),
                Arguments.of("missing details", "10.00", "STRIPE", "{}", "INVALID_DETAILS"),
                Arguments.of("malformed details", "10.00", "PAYPAL", "{\"payerEmail\":\"nope\"}", "INVALID_DETAILS"),
                Arguments.of("unknown method", "10.00", "BITCOIN", "{}", "UNSUPPORTED_METHOD"),
                Arguments.of("zero amount", "0", "STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}", "INVALID_AMOUNT"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failures")
    void aRefusedPaymentAnswersPaymentFailedAndIsKept(String label, String amount, String method, String details, String code) throws Exception {
        UUID order = UUID.randomUUID();

        processPayment(order, charge(amount, method, details));

        JsonNode data = data(awaitReply(order, "PaymentFailed"));
        assertThat(data.get("code").asString()).isEqualTo(code);
        assertThat(data.get("message").asString()).isNotBlank();
        assertThat(replies(order, "PaymentCompleted")).isEmpty();
        JsonNode payment = paymentOf(order);
        assertThat(payment.get("status").asString()).isEqualTo("FAILED");
        assertThat(payment.get("failureCode").asString()).isEqualTo(code);
    }

    // ---- redelivery and replays ---------------------------------------------------------------------------------

    @Test
    void aRedeliveredCommandIsAnsweredOnce() throws Exception {
        UUID order = UUID.randomUUID();
        UUID event = UUID.randomUUID();
        String data = charge("20.00", "STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}");

        processPayment(order, event, data);
        processPayment(order, event, data); // same Kafka message again: the inbox drops it
        UUID marker = UUID.randomUUID();
        processPayment(marker, charge("1.00", "STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}"));
        awaitReply(marker, "PaymentCompleted"); // same partition: the duplicate was consumed before this

        assertThat(replies(order, "PaymentCompleted")).hasSize(1);
    }

    @Test
    void aNewCommandForTheSameOrderNeverChargesTwice() throws Exception {
        UUID order = UUID.randomUUID();
        String data = charge("20.00", "STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}");

        processPayment(order, data);
        JsonNode first = data(awaitReply(order, "PaymentCompleted"));
        processPayment(order, data); // different event id: not an inbox duplicate, but the same order
        await().atMost(Duration.ofSeconds(30)).until(() -> replies(order, "PaymentCompleted").size() == 2);

        assertThat(db.sql("SELECT COUNT(*) FROM payments").map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
        assertThat(data(replies(order, "PaymentCompleted").get(1)).get("paymentId").asString())
                .isEqualTo(first.get("paymentId").asString());
        assertThat(data(replies(order, "PaymentCompleted").get(1)).get("reference").asString())
                .isEqualTo(first.get("reference").asString());
    }

    // ---- refunds -------------------------------------------------------------------------------------------------

    @Test
    void refundingACapturedPaymentGivesTheMoneyBack() throws Exception {
        UUID order = UUID.randomUUID();
        processPayment(order, charge("30.00", "PAYPAL", "{\"payerEmail\":\"fidele@example.com\"}"));
        awaitReply(order, "PaymentCompleted");

        refund(order);

        JsonNode data = data(awaitReply(order, "PaymentRefunded"));
        assertThat(data.get("refunded").asBoolean()).isTrue();
        assertThat(paymentOf(order).get("status").asString()).isEqualTo("REFUNDED");
    }

    @Test
    void refundingCashNotYetCollectedVoidsItWithNothingToGiveBack() throws Exception {
        UUID order = UUID.randomUUID();
        processPayment(order, charge("30.00", "CASH_ON_DELIVERY", "{\"deliveryAddress\":\"somewhere\"}"));
        awaitReply(order, "PaymentCompleted");

        refund(order);

        assertThat(data(awaitReply(order, "PaymentRefunded")).get("refunded").asBoolean()).isFalse();
        assertThat(paymentOf(order).get("status").asString()).isEqualTo("VOIDED");
    }

    @Test
    void aRefundWithNothingToRefundStillAnswers() throws Exception {
        UUID neverCharged = UUID.randomUUID();
        refund(neverCharged);
        JsonNode never = data(awaitReply(neverCharged, "PaymentRefunded"));
        assertThat(never.get("refunded").asBoolean()).isFalse();
        assertThat(never.get("paymentId").isNull()).isTrue();

        UUID failed = UUID.randomUUID();
        processPayment(failed, charge("10.00", "CREDIT_CARD", "{\"cardToken\":\"tok_declined\"}"));
        awaitReply(failed, "PaymentFailed");
        refund(failed);
        assertThat(data(awaitReply(failed, "PaymentRefunded")).get("refunded").asBoolean()).isFalse();
        assertThat(paymentOf(failed).get("status").asString()).isEqualTo("FAILED");
    }

    @Test
    void refundingTwiceGivesTheMoneyBackOnce() throws Exception {
        UUID order = UUID.randomUUID();
        processPayment(order, charge("30.00", "STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}"));
        awaitReply(order, "PaymentCompleted");

        refund(order);
        refund(order);

        await().atMost(Duration.ofSeconds(30)).until(() -> replies(order, "PaymentRefunded").size() == 2);
        assertThat(replies(order, "PaymentRefunded").stream().map(r -> data(r).get("refunded").asBoolean()))
                .containsExactly(true, false);
        assertThat(paymentOf(order).get("status").asString()).isEqualTo("REFUNDED");
    }

    // ---- HTTP ------------------------------------------------------------------------------------------------------

    @Test
    void theMethodsEndpointListsEveryStrategyWithItsRequiredDetails() {
        client.get().uri("/payments/methods").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(5)
                .jsonPath("$[?(@.method=='STRIPE')].requiredDetails[0]").isEqualTo("paymentMethodId")
                .jsonPath("$[?(@.method=='RAZORPAY')].requiredDetails.length()").isEqualTo(2);
    }

    @Test
    void anUnknownPaymentIsNotFoundAndAMalformedIdIsBadRequest() {
        client.get().uri("/payments/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("PAYMENT_NOT_FOUND");
        client.get().uri("/payments/not-a-uuid").exchange().expectStatus().isBadRequest();
    }
}
