package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        JsonNode payment = Api.payment(order);
        assertThat(payment.get("status").asString()).isEqualTo("CAPTURED");
        assertThat(payment.get("method").asString()).isEqualTo("STRIPE");
        assertThat(payment.get("amount").decimalValue()).isEqualByComparingTo("25.00");
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
        assertThat(Api.payment(order)).as("nobody was charged").isNull();
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
        assertThat(Api.payment(order)).as("nobody was charged").isNull();
    }

    @Test
    void aDeclinedCardGivesTheStockBackAndRejectsTheOrder() {
        UUID product = Api.createProduct("9.00");
        Api.setStock(product, 10, 1);

        UUID order = Api.placeOrder(product, 4, "CREDIT_CARD", "{\"cardToken\":\"tok_declined\"}");
        Api.awaitStatus(order, "REJECTED");

        assertThat(Api.lastReason(order)).isEqualTo("PAYMENT_FAILED");
        JsonNode stock = Api.stock(product);
        assertThat(stock.get("reserved").asLong()).as("the reservation was undone").isZero();
        assertThat(stock.get("free").asLong()).isEqualTo(10);
        JsonNode payment = Api.payment(order);
        assertThat(payment.get("status").asString()).isEqualTo("FAILED");
        assertThat(payment.get("failureCode").asString()).isEqualTo("CARD_DECLINED");
    }

    /** One order per payment method: the saga is the same, only the strategy behind Payment changes. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("everyMethod")
    void anOrderCanBePaidWithEveryMethod(String method, String details, String paymentStatus) {
        UUID product = Api.createProduct("6.00");
        Api.setStock(product, 10, 1);

        UUID order = Api.placeOrder(product, 1, method, details);
        Api.awaitStatus(order, "CONFIRMED");

        assertThat(Api.payment(order).get("status").asString()).isEqualTo(paymentStatus);
        assertThat(Api.payment(order).get("method").asString()).isEqualTo(method);
    }

    static Stream<Arguments> everyMethod() {
        return Stream.of(
                Arguments.of("CREDIT_CARD", "{\"cardToken\":\"tok_visa\"}", "CAPTURED"),
                Arguments.of("CASH_ON_DELIVERY", "{\"deliveryAddress\":\"12 rue des Lilas, Abidjan\"}", "PENDING_ON_DELIVERY"),
                Arguments.of("PAYPAL", "{\"payerEmail\":\"fidele@example.com\"}", "CAPTURED"),
                Arguments.of("RAZORPAY", "{\"razorpayPaymentId\":\"pay_29QQoUBi66xm2f\",\"razorpaySignature\":\"sig\"}", "CAPTURED"),
                Arguments.of("STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}", "CAPTURED"));
    }

    @Test
    void cancellingACashOnDeliveryOrderVoidsThePaymentAndFreesTheStock() {
        UUID product = Api.createProduct("6.00");
        Api.setStock(product, 10, 1);
        UUID order = Api.placeOrder(product, 2, "CASH_ON_DELIVERY", "{\"deliveryAddress\":\"somewhere\"}");
        Api.awaitStatus(order, "CONFIRMED");

        assertThat(Api.cancel(order)).isEqualTo(202);
        Api.awaitStatus(order, "CANCELLED");

        assertThat(Api.payment(order).get("status").asString()).isEqualTo("VOIDED");
        assertThat(Api.stock(product).get("reserved").asLong()).isZero();
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
        assertThat(Api.payment(order).get("status").asString()).isEqualTo("REFUNDED");
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
        assertThat(Replay.republish("order.saga.replies", order.toString(), "PaymentCompleted")).isTrue();
        Thread.sleep(5_000);

        assertThat(Api.order(order).get("status").asString()).isEqualTo("CONFIRMED");
        assertThat(Api.order(order).get("version").asLong()).isEqualTo(version);
        assertThat(Api.history(order).get("status").size()).isEqualTo(statusEntries);
        assertThat(Api.stock(product).get("reserved").asLong()).isEqualTo(3);
        assertThat(Api.payment(order).get("status").asString()).isEqualTo("CAPTURED");
    }
}
