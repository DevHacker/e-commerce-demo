package com.shop.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Persistent shapes of the order aggregate. */
public final class Records {

    private Records() {
    }

    public record Order(UUID id, String customerRef, OrderStatus status, BigDecimal total,
                        String paymentMethod, java.util.Map<String, String> paymentDetails,
                        String idempotencyKey, long version) {
    }

    public record OrderItem(UUID productId, int quantity, BigDecimal unitPrice) {
    }

    public record Saga(UUID id, SagaState state, String currentStep, long version, Instant updatedAt) {
    }

    /** Everything a decision needs, loaded at once. */
    public record Loaded(Saga saga, Order order, java.util.List<OrderItem> items) {
    }
}
