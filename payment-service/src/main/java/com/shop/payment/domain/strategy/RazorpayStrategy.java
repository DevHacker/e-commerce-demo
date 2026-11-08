package com.shop.payment.domain.strategy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.shop.payment.domain.PaymentMethod;
import com.shop.payment.domain.PaymentStatus;
import com.shop.payment.domain.strategy.ChargeResult.Approved;
import com.shop.payment.domain.strategy.ChargeResult.Declined;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;

/**
 * Razorpay checkout: the client returns a payment id and a signature, and the server verifies the signature
 * before accepting. Simulation: the signature {@code invalid} fails verification.
 */
@Component
public class RazorpayStrategy implements PaymentStrategy {

    @Override
    public PaymentMethod method() {
        return PaymentMethod.RAZORPAY;
    }

    @Override
    public Set<String> requiredDetails() {
        return Set.of("razorpayPaymentId", "razorpaySignature");
    }

    @Override
    public List<String> validate(Map<String, String> details) {
        List<String> problems = PaymentStrategy.super.validate(details);
        String id = details == null ? null : details.get("razorpayPaymentId");
        if (id != null && !id.isBlank() && !id.startsWith("pay_")) {
            problems.add("razorpayPaymentId must start with pay_");
        }
        return problems;
    }

    @Override
    public Mono<ChargeResult> charge(ChargeRequest request) {
        return Mono.fromSupplier(() -> "invalid".equals(request.details().get("razorpaySignature"))
                ? new Declined("SIGNATURE_MISMATCH", "The Razorpay signature could not be verified")
                : new Approved(request.details().get("razorpayPaymentId"), PaymentStatus.CAPTURED));
    }

    @Override
    public Mono<RefundResult> refund(RefundRequest request) {
        return Mono.fromSupplier(() -> new RefundResult.Refunded(SimulatedGateway.reference("rfnd_")));
    }
}
