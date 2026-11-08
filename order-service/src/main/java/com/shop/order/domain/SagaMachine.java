package com.shop.order.domain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.OrderItem;
import com.shop.order.domain.SagaMessages.Command;
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
import com.shop.order.domain.SagaMessages.Reply;
import com.shop.order.domain.SagaMessages.ReserveStock;
import com.shop.order.domain.SagaMessages.StockRejected;
import com.shop.order.domain.SagaMessages.StockReleased;
import com.shop.order.domain.SagaMessages.StockReserved;

/**
 * The saga orchestrator's brain: a set of pure functions from (current order and saga, what happened)
 * to a {@link Decision}. No database, no Kafka, no clock. The coordinator loads the state, calls one of
 * these functions, and applies the resulting {@link Transition} in a single transaction.
 *
 * <pre>
 * STARTED --ProductsValidated--> PRODUCTS_VALIDATED --StockReserved--> STOCK_RESERVED --PaymentCompleted--> COMPLETED
 *    |                                  |                                   |                                   |
 *    +--ProductsRejected--> REJECTED    +--StockRejected--> REJECTED        |                                 cancel
 *                                                                           |                                   |
 *   PaymentFailed: COMPENSATING(ReleaseStock) --StockReleased--> COMPENSATED, order REJECTED   <----------------+
 *   cancel:        COMPENSATING(RefundPayment) --PaymentRefunded--> COMPENSATING(ReleaseStock)
 *                  --StockReleased--> COMPENSATED, order CANCELLING -> CANCELLED
 * timeout: STARTED -> REJECTED; PRODUCTS_VALIDATED -> COMPENSATING(ReleaseStock);
 *          STOCK_RESERVED -> COMPENSATING(RefundPayment); COMPENSATING -> its current command sent again
 * </pre>
 */
public final class SagaMachine {

    private SagaMachine() {
    }

    public enum Direction { FORWARD, COMPENSATE }

    /** One line of {@code saga_step_history}. */
    public record StepEntry(String step, Direction direction, String outcome, Object detail) {
    }

    /** Everything that must change, as data. */
    public record Transition(
            SagaState state,
            String currentStep,
            Optional<OrderStatus> orderStatus,
            String reason,
            Optional<Command> command,
            Optional<EventType> event,
            List<StepEntry> steps,
            Optional<Pricing> pricing) {
    }

    public sealed interface Decision permits Apply, Ignore, Refuse {
    }

    /** Apply this transition. */
    public record Apply(Transition transition) implements Decision {
    }

    /** The message does not fit the current state (duplicate, late): do nothing. */
    public record Ignore(String why) implements Decision {
    }

    /** The request is not allowed in the current state (reported to the caller). */
    public record Refuse(String why) implements Decision {
    }

    // ---- a reply from Product or Inventory -----------------------------------------------------------

    public static Decision onReply(Loaded ctx, Reply reply) {
        SagaState state = ctx.saga().state();
        return switch (reply) {
            case ProductsValidated r when state == SagaState.STARTED -> productsValidated(ctx, r);
            case ProductsRejected r when state == SagaState.STARTED ->
                    rejected("PRODUCT_REJECTED", SagaMessages.STEP_VALIDATE_PRODUCTS, "REJECTED", r.rejections());
            case StockReserved r when state == SagaState.PRODUCTS_VALIDATED -> new Apply(new Transition(
                    SagaState.STOCK_RESERVED, SagaMessages.STEP_PROCESS_PAYMENT,
                    Optional.empty(), "STOCK_RESERVED",
                    Optional.of(new ProcessPayment(ctx.order().total(), ctx.order().paymentMethod(),
                            ctx.order().paymentDetails())),
                    Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_RESERVE_STOCK, Direction.FORWARD, "RESERVED", null)),
                    Optional.empty()));
            case StockRejected r when state == SagaState.PRODUCTS_VALIDATED ->
                    rejected("STOCK_REJECTED", SagaMessages.STEP_RESERVE_STOCK, "REJECTED", r.shortages());
            case PaymentCompleted r when state == SagaState.STOCK_RESERVED -> new Apply(new Transition(
                    SagaState.COMPLETED, SagaMessages.STEP_PROCESS_PAYMENT,
                    Optional.of(OrderStatus.CONFIRMED), "PAYMENT_" + r.status(),
                    Optional.empty(), Optional.of(EventType.OrderConfirmed),
                    List.of(new StepEntry(SagaMessages.STEP_PROCESS_PAYMENT, Direction.FORWARD, r.status(), null)),
                    Optional.empty()));
            // the stock is reserved but the customer did not pay: give it back, then reject the order
            case PaymentFailed r when state == SagaState.STOCK_RESERVED -> new Apply(new Transition(
                    SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                    Optional.empty(), "PAYMENT_FAILED",
                    Optional.of(new ReleaseStock("PAYMENT_FAILED")), Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_PROCESS_PAYMENT, Direction.FORWARD, "FAILED",
                                    Map.of("code", r.code(), "message", r.message())),
                            new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "REQUESTED", null)),
                    Optional.empty()));
            case PaymentRefunded r when state == SagaState.COMPENSATING
                    && SagaMessages.STEP_REFUND_PAYMENT.equals(ctx.saga().currentStep()) -> new Apply(new Transition(
                    SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                    Optional.empty(), "PAYMENT_REFUNDED",
                    Optional.of(new ReleaseStock("PAYMENT_REFUNDED")), Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_REFUND_PAYMENT, Direction.COMPENSATE,
                                    r.refunded() ? "REFUNDED" : "NOTHING_TO_REFUND", null),
                            new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "REQUESTED", null)),
                    Optional.empty()));
            case StockReleased r when state == SagaState.COMPENSATING
                    && SagaMessages.STEP_RELEASE_STOCK.equals(ctx.saga().currentStep()) -> stockReleased(ctx);
            default -> new Ignore(reply.getClass().getSimpleName() + " is not expected in saga state " + state);
        };
    }

    private static Decision productsValidated(Loaded ctx, ProductsValidated reply) {
        Optional<Pricing> pricing = Pricing.of(ctx.items(), reply.prices());
        if (pricing.isEmpty()) {
            return rejected("PRICE_MISSING", SagaMessages.STEP_VALIDATE_PRODUCTS, "REJECTED",
                    "Product service answered without a price for every item");
        }
        return new Apply(new Transition(
                SagaState.PRODUCTS_VALIDATED, SagaMessages.STEP_RESERVE_STOCK,
                Optional.empty(), "PRODUCTS_VALIDATED",
                Optional.of(new ReserveStock(lines(ctx.items()))), Optional.empty(),
                List.of(new StepEntry(SagaMessages.STEP_VALIDATE_PRODUCTS, Direction.FORWARD, "VALIDATED", null)),
                pricing));
    }

    /** A cancelled order ends CANCELLED; an order that was never confirmed (payment failed) ends REJECTED. */
    private static Decision stockReleased(Loaded ctx) {
        boolean cancelled = ctx.order().status() == OrderStatus.CANCELLING;
        return new Apply(new Transition(
                SagaState.COMPENSATED, SagaMessages.STEP_RELEASE_STOCK,
                Optional.of(cancelled ? OrderStatus.CANCELLED : OrderStatus.REJECTED),
                cancelled ? "STOCK_RELEASED" : "PAYMENT_FAILED",
                Optional.empty(), Optional.of(cancelled ? EventType.OrderCancelled : EventType.OrderRejected),
                List.of(new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "RELEASED", null)),
                Optional.empty()));
    }

    // ---- cancellation requested by the customer -------------------------------------------------------

    /** Only a confirmed order can be cancelled; that starts the compensation: refund first, then release the stock. */
    public static Decision cancel(Loaded ctx) {
        if (ctx.order().status() != OrderStatus.CONFIRMED || ctx.saga().state() != SagaState.COMPLETED) {
            return new Refuse("Order is " + ctx.order().status() + "; only a CONFIRMED order can be cancelled");
        }
        return new Apply(new Transition(
                SagaState.COMPENSATING, SagaMessages.STEP_REFUND_PAYMENT,
                Optional.of(OrderStatus.CANCELLING), "CANCEL_REQUESTED",
                Optional.of(new RefundPayment("ORDER_CANCELLED")), Optional.empty(),
                List.of(new StepEntry(SagaMessages.STEP_REFUND_PAYMENT, Direction.COMPENSATE, "REQUESTED", null)),
                Optional.empty()));
    }

    // ---- the saga made no progress for too long -------------------------------------------------------

    public static Decision timeout(Loaded ctx) {
        return switch (ctx.saga().state()) {
            // nothing was reserved yet: reject
            case STARTED -> rejected("TIMEOUT", SagaMessages.STEP_VALIDATE_PRODUCTS, "TIMED_OUT",
                    "No answer from the Product service");
            // ReserveStock was sent, a reservation may exist: compensate
            case PRODUCTS_VALIDATED -> new Apply(new Transition(
                    SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                    Optional.of(OrderStatus.CANCELLING), "TIMEOUT",
                    Optional.of(new ReleaseStock("TIMEOUT")), Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_RESERVE_STOCK, Direction.FORWARD, "TIMED_OUT", null),
                            new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "REQUESTED", null)),
                    Optional.empty()));
            // ProcessPayment was sent, money may have been taken: refund first (a refund with nothing to refund is a no-op)
            case STOCK_RESERVED -> new Apply(new Transition(
                    SagaState.COMPENSATING, SagaMessages.STEP_REFUND_PAYMENT,
                    Optional.of(OrderStatus.CANCELLING), "TIMEOUT",
                    Optional.of(new RefundPayment("TIMEOUT")), Optional.empty(),
                    List.of(new StepEntry(SagaMessages.STEP_PROCESS_PAYMENT, Direction.FORWARD, "TIMED_OUT", null),
                            new StepEntry(SagaMessages.STEP_REFUND_PAYMENT, Direction.COMPENSATE, "REQUESTED", null)),
                    Optional.empty()));
            // the current compensation got no answer: ask again (both Payment's refund and Inventory's release are idempotent)
            case COMPENSATING -> SagaMessages.STEP_REFUND_PAYMENT.equals(ctx.saga().currentStep())
                    ? new Apply(new Transition(
                            SagaState.COMPENSATING, SagaMessages.STEP_REFUND_PAYMENT,
                            Optional.empty(), "REFUND_RETRIED",
                            Optional.of(new RefundPayment("TIMEOUT")), Optional.empty(),
                            List.of(new StepEntry(SagaMessages.STEP_REFUND_PAYMENT, Direction.COMPENSATE, "RETRIED", null)),
                            Optional.empty()))
                    : new Apply(new Transition(
                            SagaState.COMPENSATING, SagaMessages.STEP_RELEASE_STOCK,
                            Optional.empty(), "RELEASE_RETRIED",
                            Optional.of(new ReleaseStock("TIMEOUT")), Optional.empty(),
                            List.of(new StepEntry(SagaMessages.STEP_RELEASE_STOCK, Direction.COMPENSATE, "RETRIED", null)),
                            Optional.empty()));
            case COMPLETED, REJECTED, COMPENSATED -> new Ignore("Saga is " + ctx.saga().state() + ", nothing to time out");
        };
    }

    // ---- helpers ----------------------------------------------------------------------------------------

    private static Decision rejected(String reason, String step, String outcome, Object detail) {
        return new Apply(new Transition(
                SagaState.REJECTED, step,
                Optional.of(OrderStatus.REJECTED), reason,
                Optional.empty(), Optional.of(EventType.OrderRejected),
                List.of(new StepEntry(step, Direction.FORWARD, outcome, detail)),
                Optional.empty()));
    }

    private static List<Line> lines(List<OrderItem> items) {
        return items.stream().map(i -> new Line(i.productId(), i.quantity())).toList();
    }
}
