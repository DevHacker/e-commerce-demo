package com.shop.payment.domain.strategy;

import java.math.BigDecimal;

import com.shop.payment.domain.PaymentStatus;

/** What a strategy needs to undo a payment it approved earlier. */
public record RefundRequest(String reference, BigDecimal amount, PaymentStatus status) {
}
