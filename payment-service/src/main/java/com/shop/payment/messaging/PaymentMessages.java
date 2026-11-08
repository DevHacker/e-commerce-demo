package com.shop.payment.messaging;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

/** Topics, message types and payloads (the {@code data} part of the envelope) of the Payment service. */
public final class PaymentMessages {

    public static final String COMMANDS_TOPIC = "payment.commands";
    public static final String PROCESS_PAYMENT = "ProcessPayment";
    public static final String REFUND_PAYMENT = "RefundPayment";
    public static final String PAYMENT_COMPLETED = "PaymentCompleted";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String PAYMENT_REFUNDED = "PaymentRefunded";

    private PaymentMessages() {
    }

    /** The method name is the order's choice; {@code details} are that method's fields. */
    public record ProcessPaymentData(BigDecimal amount, String method, Map<String, String> details) {
    }

    /** {@code reason} is informational (ORDER_CANCELLED, TIMEOUT). */
    public record RefundPaymentData(String reason) {
    }

    public record PaymentCompletedData(UUID paymentId, String method, String status, String reference) {
    }

    public record PaymentFailedData(String code, String message) {
    }

    public record PaymentRefundedData(boolean refunded, UUID paymentId) {
    }
}
