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
 * Stripe payment with a payment method id created by Stripe.js. Simulation, using Stripe's own test ids:
 * {@code pm_card_chargeDeclined} and {@code pm_card_chargeDeclinedInsufficientFunds} are refused.
 */
@Component
public class StripeStrategy implements PaymentStrategy {

    @Override
    public PaymentMethod method() {
        return PaymentMethod.STRIPE;
    }

    @Override
    public Set<String> requiredDetails() {
        return Set.of("paymentMethodId");
    }

    @Override
    public List<String> validate(Map<String, String> details) {
        List<String> problems = PaymentStrategy.super.validate(details);
        String id = details == null ? null : details.get("paymentMethodId");
        if (id != null && !id.isBlank() && !id.startsWith("pm_")) {
            problems.add("paymentMethodId must start with pm_");
        }
        return problems;
    }

    @Override
    public Mono<ChargeResult> charge(ChargeRequest request) {
        return Mono.fromSupplier(() -> switch (request.details().get("paymentMethodId")) {
            case "pm_card_chargeDeclined" -> new Declined("CARD_DECLINED", "Your card was declined");
            case "pm_card_chargeDeclinedInsufficientFunds" ->
                    new Declined("INSUFFICIENT_FUNDS", "Your card has insufficient funds");
            default -> new Approved(SimulatedGateway.reference("ch_"), PaymentStatus.CAPTURED);
        });
    }

    @Override
    public Mono<RefundResult> refund(RefundRequest request) {
        return Mono.fromSupplier(() -> new RefundResult.Refunded(SimulatedGateway.reference("re_")));
    }
}
