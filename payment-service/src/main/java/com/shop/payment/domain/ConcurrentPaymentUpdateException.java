package com.shop.payment.domain;

import java.util.UUID;

/** A payment changed between read and write; the message is retried on fresh state. */
public class ConcurrentPaymentUpdateException extends RuntimeException {

    public ConcurrentPaymentUpdateException(UUID paymentId) {
        super("Payment " + paymentId + " was modified concurrently");
    }
}
