package com.shop.order.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.domain.OrderStatus;
import tools.jackson.databind.JsonNode;

/** Wire format of the HTTP API. */
public final class OrderDtos {

    private OrderDtos() {
    }

    /** {@code method}: CREDIT_CARD, CASH_ON_DELIVERY, PAYPAL, RAZORPAY or STRIPE. {@code details}: what that method needs (tokens, never card numbers). */
    public record PaymentRequest(String method, Map<String, String> details) {
    }

    public record CreateOrderRequest(String customerRef, List<ItemRequest> items, PaymentRequest payment) {
    }

    public record ItemRequest(UUID productId, Integer quantity) {
    }

    public record AcceptedResponse(UUID orderId, OrderStatus status) {
    }

    public record ItemResponse(UUID productId, int quantity, BigDecimal unitPrice) {
    }

    /** {@code total} and {@code unitPrice} are null until the Product service has answered. */
    public record OrderResponse(UUID id, String customerRef, OrderStatus status, BigDecimal total,
                                String paymentMethod, List<ItemResponse> items, long version) {
    }

    public record StatusEntryResponse(OrderStatus from, OrderStatus to, String reason, UUID triggerMessageId,
                                      Instant occurredAt) {
    }

    public record StepEntryResponse(String step, String direction, String outcome, UUID messageId, JsonNode detail,
                                    Instant occurredAt) {
    }

    public record HistoryResponse(UUID orderId, List<StatusEntryResponse> status, List<StepEntryResponse> steps) {
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
