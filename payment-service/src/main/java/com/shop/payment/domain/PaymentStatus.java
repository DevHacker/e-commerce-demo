package com.shop.payment.domain;

public enum PaymentStatus {
    /** Money taken. */
    CAPTURED,
    /** Cash on delivery: accepted, cash to be collected by the courier. */
    PENDING_ON_DELIVERY,
    FAILED,
    /** Money given back. */
    REFUNDED,
    /** Accepted but nothing had been collected, so nothing to give back. */
    VOIDED;

    /** Only these can be undone by a refund request. */
    public boolean refundable() {
        return this == CAPTURED || this == PENDING_ON_DELIVERY;
    }
}
