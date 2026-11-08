package com.shop.payment.domain;

import java.math.BigDecimal;
import java.util.UUID;

/** A payment as stored: one per order. */
public record Payment(
        UUID id,
        UUID orderId,
        String method,
        BigDecimal amount,
        PaymentStatus status,
        String reference,
        String failureCode,
        String failureMessage,
        long version) {
}
