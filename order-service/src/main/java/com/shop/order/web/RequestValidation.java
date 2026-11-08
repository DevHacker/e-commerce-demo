package com.shop.order.web;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.shop.order.web.OrderDtos.CreateOrderRequest;
import com.shop.order.web.OrderDtos.ItemRequest;

/** Pure request checks: the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_ITEMS = 100;
    static final int MAX_QUANTITY = 10_000;
    static final int MAX_TEXT = 100;
    static final int MAX_DETAILS = 20;
    static final int MAX_DETAIL_TEXT = 200;
    /** The payment methods the Payment service has a strategy for. */
    static final Set<String> PAYMENT_METHODS =
            Set.of("CREDIT_CARD", "CASH_ON_DELIVERY", "PAYPAL", "RAZORPAY", "STRIPE");

    private RequestValidation() {
    }

    static List<String> check(String idempotencyKey, CreateOrderRequest request) {
        List<String> errors = new ArrayList<>();
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            errors.add("Idempotency-Key header is required");
        } else if (idempotencyKey.length() > MAX_TEXT) {
            errors.add("Idempotency-Key must be at most " + MAX_TEXT + " characters");
        }
        if (request.customerRef() == null || request.customerRef().isBlank()) {
            errors.add("customerRef is required");
        } else if (request.customerRef().length() > MAX_TEXT) {
            errors.add("customerRef must be at most " + MAX_TEXT + " characters");
        }
        checkPayment(request.payment(), errors);
        List<ItemRequest> items = request.items();
        if (items == null || items.isEmpty()) {
            errors.add("items must contain at least one item");
            return errors;
        }
        if (items.size() > MAX_ITEMS) {
            errors.add("items must contain at most " + MAX_ITEMS + " items");
        }
        Set<UUID> seen = new HashSet<>();
        for (int i = 0; i < items.size(); i++) {
            ItemRequest item = items.get(i);
            if (item == null || item.productId() == null) {
                errors.add("items[" + i + "].productId is required");
                continue;
            }
            if (!seen.add(item.productId())) {
                errors.add("items[" + i + "].productId appears more than once");
            }
            if (item.quantity() == null || item.quantity() < 1 || item.quantity() > MAX_QUANTITY) {
                errors.add("items[" + i + "].quantity must be between 1 and " + MAX_QUANTITY);
            }
        }
        return errors;
    }

    private static void checkPayment(OrderDtos.PaymentRequest payment, List<String> errors) {
        if (payment == null || payment.method() == null || payment.method().isBlank()) {
            errors.add("payment.method is required");
            return;
        }
        if (!PAYMENT_METHODS.contains(normalizedMethod(payment))) {
            errors.add("payment.method must be one of " + new java.util.TreeSet<>(PAYMENT_METHODS));
        }
        Map<String, String> details = payment.details();
        if (details == null) {
            return;
        }
        if (details.size() > MAX_DETAILS) {
            errors.add("payment.details must contain at most " + MAX_DETAILS + " entries");
        }
        details.forEach((k, v) -> {
            if (k == null || k.isBlank() || v == null || v.isBlank()) {
                errors.add("payment.details entries must have a non-blank name and value");
            } else if (v.length() > MAX_DETAIL_TEXT) {
                errors.add("payment.details." + k + " must be at most " + MAX_DETAIL_TEXT + " characters");
            }
        });
    }

    /** CREDIT_CARD, credit_card and Credit_Card are the same method. */
    static String normalizedMethod(OrderDtos.PaymentRequest payment) {
        return payment.method().trim().toUpperCase(java.util.Locale.ROOT);
    }
}
