package com.shop.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.Records.OrderItem;
import com.shop.order.domain.Records.Saga;
import com.shop.order.domain.SagaMachine.Apply;
import com.shop.order.domain.SagaMachine.Decision;
import com.shop.order.domain.SagaMachine.Direction;
import com.shop.order.domain.SagaMachine.Ignore;
import com.shop.order.domain.SagaMachine.Refuse;
import com.shop.order.domain.SagaMachine.Transition;
import com.shop.order.domain.SagaMessages.EventType;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.domain.SagaMessages.PaymentCompleted;
import com.shop.order.domain.SagaMessages.PaymentFailed;
import com.shop.order.domain.SagaMessages.PaymentRefunded;
import com.shop.order.domain.SagaMessages.ProcessPayment;
import com.shop.order.domain.SagaMessages.ProductsRejected;
import com.shop.order.domain.SagaMessages.ProductsValidated;
import com.shop.order.domain.SagaMessages.RefundPayment;
import com.shop.order.domain.SagaMessages.ReleaseStock;
import com.shop.order.domain.SagaMessages.ReserveStock;
import com.shop.order.domain.SagaMessages.StockRejected;
import com.shop.order.domain.SagaMessages.StockReleased;
import com.shop.order.domain.SagaMessages.StockReserved;
import org.junit.jupiter.api.Test;

class SagaMachineTest {

    private static final UUID ORDER = UUID.randomUUID();
    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    private static Loaded loaded(SagaState saga, OrderStatus order) {
        return loaded(saga, order, "X");
    }

    private static Loaded loaded(SagaState saga, OrderStatus order, String step) {
        return new Loaded(
                new Saga(ORDER, saga, step, 3, Instant.now()),
                new Order(ORDER, "cust-1", order, new BigDecimal("25.50"), "STRIPE", Map.of("paymentMethodId", "pm_x"),
                        "key", 5),
                List.of(new OrderItem(A, 2, null), new OrderItem(B, 1, null)));
    }

    private static Transition applied(Decision decision) {
        assertThat(decision).isInstanceOf(Apply.class);
        return ((Apply) decision).transition();
    }

    // ---- nominal path ------------------------------------------------------------------------------

    @Test
    void productsValidatedPricesTheOrderAndAsksInventoryToReserve() {
        var prices = Map.of(A, new BigDecimal("10.00"), B, new BigDecimal("5.50"));

        Transition t = applied(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING),
                new ProductsValidated(prices)));

        assertThat(t.state()).isEqualTo(SagaState.PRODUCTS_VALIDATED);
        assertThat(t.orderStatus()).isEmpty();
        assertThat(t.command()).contains(new ReserveStock(List.of(new Line(A, 2), new Line(B, 1))));
        assertThat(t.pricing().orElseThrow().total()).isEqualByComparingTo("25.50");
        assertThat(t.steps()).singleElement().satisfies(s -> {
            assertThat(s.step()).isEqualTo("ValidateProducts");
            assertThat(s.direction()).isEqualTo(Direction.FORWARD);
            assertThat(s.outcome()).isEqualTo("VALIDATED");
        });
        assertThat(t.event()).isEmpty();
    }

    @Test
    void stockReservedAsksPaymentToChargeTheSnapshotTotalWithTheChosenMethod() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING), new StockReserved()));

        assertThat(t.state()).isEqualTo(SagaState.STOCK_RESERVED);
        assertThat(t.orderStatus()).isEmpty();
        assertThat(t.command()).contains(new ProcessPayment(new BigDecimal("25.50"), "STRIPE", Map.of("paymentMethodId", "pm_x")));
        assertThat(t.steps()).extracting(SagaMachine.StepEntry::outcome).containsExactly("RESERVED");
    }

    @Test
    void paymentCompletedConfirmsTheOrder() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.STOCK_RESERVED, OrderStatus.PENDING), new PaymentCompleted("CAPTURED")));

        assertThat(t.state()).isEqualTo(SagaState.COMPLETED);
        assertThat(t.orderStatus()).contains(OrderStatus.CONFIRMED);
        assertThat(t.event()).contains(EventType.OrderConfirmed);
        assertThat(t.command()).isEmpty();
    }

    @Test
    void cashOnDeliveryConfirmsTheOrderToo() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.STOCK_RESERVED, OrderStatus.PENDING), new PaymentCompleted("PENDING_ON_DELIVERY")));

        assertThat(t.orderStatus()).contains(OrderStatus.CONFIRMED);
        assertThat(t.steps()).extracting(SagaMachine.StepEntry::outcome).containsExactly("PENDING_ON_DELIVERY");
    }

    // ---- failures ------------------------------------------------------------------------------------

    @Test
    void productsRejectedRejectsTheOrderWithoutCompensation() {
        Transition t = applied(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING),
                new ProductsRejected(List.of("NOT_FOUND"))));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.orderStatus()).contains(OrderStatus.REJECTED);
        assertThat(t.event()).contains(EventType.OrderRejected);
        assertThat(t.command()).isEmpty();
        assertThat(t.steps().get(0).detail()).isEqualTo(List.of("NOT_FOUND"));
    }

    @Test
    void stockRejectedRejectsTheOrderWithoutCompensation() {
        Transition t = applied(SagaMachine.onReply(loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING),
                new StockRejected(List.of("short"))));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.orderStatus()).contains(OrderStatus.REJECTED);
        assertThat(t.reason()).isEqualTo("STOCK_REJECTED");
        assertThat(t.command()).isEmpty();
    }

    @Test
    void aMissingPriceRejectsTheOrder() {
        Transition t = applied(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING),
                new ProductsValidated(Map.of(A, BigDecimal.ONE))));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.reason()).isEqualTo("PRICE_MISSING");
    }

    @Test
    void paymentFailedGivesTheStockBackThenRejectsTheOrder() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.STOCK_RESERVED, OrderStatus.PENDING), new PaymentFailed("CARD_DECLINED", "declined")));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.orderStatus()).isEmpty();
        assertThat(t.command()).contains(new ReleaseStock("PAYMENT_FAILED"));
        assertThat(t.steps()).extracting(SagaMachine.StepEntry::outcome).containsExactly("FAILED", "REQUESTED");
    }

    @Test
    void stockReleasedAfterAFailedPaymentRejectsTheOrder() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.COMPENSATING, OrderStatus.PENDING, "ReleaseStock"), new StockReleased()));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATED);
        assertThat(t.orderStatus()).contains(OrderStatus.REJECTED);
        assertThat(t.reason()).isEqualTo("PAYMENT_FAILED");
        assertThat(t.event()).contains(EventType.OrderRejected);
    }

    // ---- cancellation ----------------------------------------------------------------------------------

    @Test
    void cancelOfAConfirmedOrderStartsWithTheRefund() {
        Transition t = applied(SagaMachine.cancel(loaded(SagaState.COMPLETED, OrderStatus.CONFIRMED)));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.currentStep()).isEqualTo("RefundPayment");
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLING);
        assertThat(t.command()).contains(new RefundPayment("ORDER_CANCELLED"));
        assertThat(t.steps().get(0).direction()).isEqualTo(Direction.COMPENSATE);
    }

    @Test
    void theRefundIsFollowedByTheStockRelease() {
        for (boolean refunded : List.of(true, false)) {
            Transition t = applied(SagaMachine.onReply(
                    loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING, "RefundPayment"), new PaymentRefunded(refunded)));

            assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
            assertThat(t.currentStep()).isEqualTo("ReleaseStock");
            assertThat(t.command()).contains(new ReleaseStock("PAYMENT_REFUNDED"));
            assertThat(t.orderStatus()).isEmpty();
            assertThat(t.steps().get(0).outcome()).isEqualTo(refunded ? "REFUNDED" : "NOTHING_TO_REFUND");
        }
    }

    @Test
    void stockReleasedCompletesTheCancellation() {
        Transition t = applied(SagaMachine.onReply(
                loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING, "ReleaseStock"), new StockReleased()));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATED);
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLED);
        assertThat(t.event()).contains(EventType.OrderCancelled);
    }

    @Test
    void onlyAConfirmedOrderCanBeCancelled() {
        for (OrderStatus status : List.of(OrderStatus.PENDING, OrderStatus.REJECTED, OrderStatus.CANCELLING,
                OrderStatus.CANCELLED)) {
            assertThat(SagaMachine.cancel(loaded(SagaState.STARTED, status))).isInstanceOf(Refuse.class);
        }
    }

    // ---- timeout -----------------------------------------------------------------------------------------

    @Test
    void aStartedSagaThatTimesOutIsRejected() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.STARTED, OrderStatus.PENDING)));

        assertThat(t.state()).isEqualTo(SagaState.REJECTED);
        assertThat(t.reason()).isEqualTo("TIMEOUT");
        assertThat(t.event()).contains(EventType.OrderRejected);
        assertThat(t.command()).isEmpty();
    }

    @Test
    void aSagaWaitingForStockThatTimesOutIsCompensated() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING)));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLING);
        assertThat(t.command()).contains(new ReleaseStock("TIMEOUT"));
        assertThat(t.steps()).extracting(SagaMachine.StepEntry::outcome).containsExactly("TIMED_OUT", "REQUESTED");
    }

    @Test
    void aSagaWaitingForPaymentThatTimesOutIsRefundedFirst() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.STOCK_RESERVED, OrderStatus.PENDING)));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.currentStep()).isEqualTo("RefundPayment");
        assertThat(t.orderStatus()).contains(OrderStatus.CANCELLING);
        assertThat(t.command()).contains(new RefundPayment("TIMEOUT"));
        assertThat(t.steps()).extracting(SagaMachine.StepEntry::outcome).containsExactly("TIMED_OUT", "REQUESTED");
    }

    @Test
    void aReleaseWithoutAnswerAsksAgainAndKeepsTheStatus() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING, "ReleaseStock")));

        assertThat(t.state()).isEqualTo(SagaState.COMPENSATING);
        assertThat(t.orderStatus()).isEqualTo(Optional.empty());
        assertThat(t.command()).contains(new ReleaseStock("TIMEOUT"));
    }

    @Test
    void aRefundWithoutAnswerAsksAgain() {
        Transition t = applied(SagaMachine.timeout(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING, "RefundPayment")));

        assertThat(t.currentStep()).isEqualTo("RefundPayment");
        assertThat(t.command()).contains(new RefundPayment("TIMEOUT"));
    }

    @Test
    void finishedSagasDoNotTimeOut() {
        for (SagaState state : List.of(SagaState.COMPLETED, SagaState.REJECTED, SagaState.COMPENSATED)) {
            assertThat(SagaMachine.timeout(loaded(state, OrderStatus.CONFIRMED))).isInstanceOf(Ignore.class);
        }
    }

    // ---- duplicates and late replies -----------------------------------------------------------------------

    @Test
    void repliesThatDoNotFitTheStateAreIgnored() {
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPLETED, OrderStatus.CONFIRMED), new StockReserved()))
                .isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.STARTED, OrderStatus.PENDING), new StockReserved()))
                .isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING), new StockReserved()))
                .isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.REJECTED, OrderStatus.REJECTED),
                new ProductsValidated(Map.of(A, BigDecimal.ONE, B, BigDecimal.ONE)))).isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.PRODUCTS_VALIDATED, OrderStatus.PENDING), new StockReleased()))
                .isInstanceOf(Ignore.class);
        // a late payment answer after the saga moved on, a release answer while the refund is still pending
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING, "RefundPayment"),
                new PaymentCompleted("CAPTURED"))).isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPENSATING, OrderStatus.CANCELLING, "RefundPayment"),
                new StockReleased())).isInstanceOf(Ignore.class);
        assertThat(SagaMachine.onReply(loaded(SagaState.COMPLETED, OrderStatus.CONFIRMED), new PaymentFailed("X", "y")))
                .isInstanceOf(Ignore.class);
    }
}
