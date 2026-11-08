package com.shop.payment.domain.strategy;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

/** What a strategy needs to take a payment. {@code details} are the method's own fields (tokens, never card numbers). */
public record ChargeRequest(UUID orderId, BigDecimal amount, Map<String, String> details) {
}
