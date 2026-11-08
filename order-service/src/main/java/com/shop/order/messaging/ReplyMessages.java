package com.shop.order.messaging;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Payloads (the {@code data} of the envelope) of the replies on {@code order.saga.replies}. */
public final class ReplyMessages {

    public static final String TOPIC = "order.saga.replies";
    public static final String PRODUCTS_VALIDATED = "ProductsValidated";
    public static final String PRODUCTS_REJECTED = "ProductsRejected";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";
    public static final String STOCK_RELEASED = "StockReleased";
    public static final String PAYMENT_COMPLETED = "PaymentCompleted";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String PAYMENT_REFUNDED = "PaymentRefunded";

    private ReplyMessages() {
    }

    public record ProductsValidatedData(List<Priced> items) {

        public record Priced(UUID productId, BigDecimal unitPrice) {
        }
    }

    public record ProductsRejectedData(List<Rejection> rejections) {

        public record Rejection(UUID productId, String reason) {
        }
    }

    public record PaymentCompletedData(String status) {
    }

    public record PaymentFailedData(String code, String message) {
    }

    public record PaymentRefundedData(boolean refunded) {
    }

    public record StockRejectedData(List<Shortage> shortages) {

        public record Shortage(UUID productId, long requested, long missing) {
        }
    }
}
