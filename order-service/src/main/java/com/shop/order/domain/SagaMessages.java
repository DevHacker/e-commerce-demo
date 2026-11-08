package com.shop.order.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** What the saga receives from the other services, and what it sends. Pure data. */
public final class SagaMessages {

    private SagaMessages() {
    }

    public static final String PRODUCT_COMMANDS = "product.commands";
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String SAGA_REPLIES = "order.saga.replies";
    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String ORDER_EVENTS = "order.events";

    public static final String STEP_VALIDATE_PRODUCTS = "ValidateProducts";
    public static final String STEP_RESERVE_STOCK = "ReserveStock";
    public static final String STEP_RELEASE_STOCK = "ReleaseStock";
    public static final String STEP_PROCESS_PAYMENT = "ProcessPayment";
    public static final String STEP_REFUND_PAYMENT = "RefundPayment";

    // ---- replies --------------------------------------------------------------------------------------

    public sealed interface Reply permits ProductsValidated, ProductsRejected, StockReserved, StockRejected,
            StockReleased, PaymentCompleted, PaymentFailed, PaymentRefunded {
    }

    /** Current prices per product, as answered by Product. */
    public record ProductsValidated(Map<UUID, BigDecimal> prices) implements Reply {
    }

    public record ProductsRejected(List<String> rejections) implements Reply {
    }

    public record StockReserved() implements Reply {
    }

    public record StockRejected(List<String> shortages) implements Reply {
    }

    public record StockReleased() implements Reply {
    }

    /** {@code status}: CAPTURED (money taken) or PENDING_ON_DELIVERY (cash to collect). */
    public record PaymentCompleted(String status) implements Reply {
    }

    public record PaymentFailed(String code, String message) implements Reply {
    }

    /** {@code refunded} is false when there was nothing to give back (never charged, or cash not collected). */
    public record PaymentRefunded(boolean refunded) implements Reply {
    }

    // ---- commands --------------------------------------------------------------------------------------

    public record Line(UUID productId, int quantity) {
    }

    public sealed interface Command permits ValidateProducts, ReserveStock, ReleaseStock, ProcessPayment, RefundPayment {

        String topic();

        String type();

        Object data();
    }

    public record ValidateProducts(List<Line> items) implements Command {

        public String topic() {
            return PRODUCT_COMMANDS;
        }

        public String type() {
            return STEP_VALIDATE_PRODUCTS;
        }

        public Object data() {
            return Map.of("items", items);
        }
    }

    public record ReserveStock(List<Line> items) implements Command {

        public String topic() {
            return INVENTORY_COMMANDS;
        }

        public String type() {
            return STEP_RESERVE_STOCK;
        }

        public Object data() {
            return Map.of("items", items);
        }
    }

    public record ReleaseStock(String reason) implements Command {

        public String topic() {
            return INVENTORY_COMMANDS;
        }

        public String type() {
            return STEP_RELEASE_STOCK;
        }

        public Object data() {
            return Map.of("reason", reason);
        }
    }

    public record ProcessPayment(BigDecimal amount, String method, Map<String, String> details) implements Command {

        public String topic() {
            return PAYMENT_COMMANDS;
        }

        public String type() {
            return STEP_PROCESS_PAYMENT;
        }

        public Object data() {
            return Map.of("amount", amount, "method", method, "details", details);
        }
    }

    public record RefundPayment(String reason) implements Command {

        public String topic() {
            return PAYMENT_COMMANDS;
        }

        public String type() {
            return STEP_REFUND_PAYMENT;
        }

        public Object data() {
            return Map.of("reason", reason);
        }
    }

    // ---- domain events on order.events ------------------------------------------------------------------

    public enum EventType { OrderCreated, OrderConfirmed, OrderRejected, OrderCancelled }
}
