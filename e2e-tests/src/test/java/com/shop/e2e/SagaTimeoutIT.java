package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A service is down: the order service gives up on the saga after its timeout (20 s here) and compensates. */
class SagaTimeoutIT {

    @BeforeAll
    static void stack() {
        Stack.start();
    }

    @AfterAll
    static void bringEveryoneBack() {
        Stack.restartService("product");
        Stack.restartService("payment");
    }

    @Test
    void silenceFromProductRejectsTheOrderAndALateAnswerChangesNothing() throws Exception {
        UUID product = Api.createProduct("4.00");
        Api.setStock(product, 10, 1);
        Stack.stopService("product");

        UUID order = Api.placeOrder(product, 1);
        assertThat(Api.awaitFinal(order, Duration.ofSeconds(90))).isEqualTo("REJECTED");
        assertThat(Api.lastReason(order)).isEqualTo("TIMEOUT");

        // Product comes back, finds the old command, answers: the reply is late and must be ignored.
        Stack.restartService("product");
        Thread.sleep(8_000);
        assertThat(Api.order(order).get("status").asString()).isEqualTo("REJECTED");
        assertThat(Api.stock(product).get("reserved").asLong()).isZero();
    }

    @Test
    void paymentDownAfterTheStockWasReservedEndsCancelledOnceItIsBackWithTheStockFree() throws Exception {
        UUID product = Api.createProduct("4.00");
        Api.setStock(product, 10, 1);
        Stack.stopService("payment");

        UUID order = Api.placeOrder(product, 3);
        // stock is reserved, ProcessPayment waits in Kafka, the saga times out and starts to compensate
        Api.awaitStatus(order, "CANCELLING");
        assertThat(Api.stock(product).get("reserved").asLong()).as("not released before the refund is answered").isEqualTo(3);

        // Payment returns: it finds ProcessPayment (charges), then RefundPayment (refunds), then the stock is released
        Stack.restartService("payment");
        Api.awaitStatus(order, "CANCELLED");

        assertThat(Api.stock(product).get("reserved").asLong()).isZero();
        assertThat(Api.payment(order).get("status").asString()).isEqualTo("REFUNDED");
    }
}
