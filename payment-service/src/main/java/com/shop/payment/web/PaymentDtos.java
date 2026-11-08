package com.shop.payment.web;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.shop.payment.domain.Payment;

/** Wire format of the read-only HTTP API. */
public final class PaymentDtos {

    private PaymentDtos() {
    }

    public record PaymentResponse(UUID paymentId, UUID orderId, String method, BigDecimal amount, String status,
                                  String reference, String failureCode, String failureMessage) {

        public static PaymentResponse from(Payment p) {
            return new PaymentResponse(p.id(), p.orderId(), p.method(), p.amount(), p.status().name(),
                    p.reference(), p.failureCode(), p.failureMessage());
        }
    }

    public record MethodResponse(String method, List<String> requiredDetails) {
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
