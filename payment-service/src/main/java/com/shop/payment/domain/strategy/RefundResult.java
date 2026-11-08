package com.shop.payment.domain.strategy;

/** Money went back ({@link Refunded}) or there was none to give back ({@link Voided}). */
public sealed interface RefundResult {

    record Refunded(String reference) implements RefundResult {
    }

    record Voided() implements RefundResult {
    }
}
