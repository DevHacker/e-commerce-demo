package com.shop.payment.domain.strategy;

import com.shop.payment.domain.PaymentStatus;

/** The gateway's answer to a charge: approved (with the status it leaves the payment in) or declined. */
public sealed interface ChargeResult {

    record Approved(String reference, PaymentStatus status) implements ChargeResult {
    }

    record Declined(String code, String message) implements ChargeResult {
    }
}
