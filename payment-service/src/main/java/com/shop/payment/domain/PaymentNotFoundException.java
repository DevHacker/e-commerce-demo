package com.shop.payment.domain;

import java.util.UUID;

public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(UUID orderId) {
        super("No payment for order " + orderId);
    }
}
