package com.shop.payment.domain;

import java.util.Locale;
import java.util.Optional;

/** The payment methods the platform accepts. Each one has exactly one {@code PaymentStrategy}. */
public enum PaymentMethod {
    CREDIT_CARD, CASH_ON_DELIVERY, PAYPAL, RAZORPAY, STRIPE;

    /** "credit_card", "Credit_Card" and "CREDIT_CARD" are the same method. */
    public static Optional<PaymentMethod> parse(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(name.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
