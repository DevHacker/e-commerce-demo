package com.shop.payment.domain.strategy;

import java.util.Set;

import com.shop.payment.domain.PaymentMethod;
import com.shop.payment.domain.PaymentStatus;
import com.shop.payment.domain.strategy.ChargeResult.Approved;
import com.shop.payment.domain.strategy.ChargeResult.Declined;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;

/**
 * Card payment through a card token (the client tokenises the card; this service never sees a number).
 * Simulation: {@code tok_declined} and {@code tok_insufficient_funds} are refused, any other token is charged.
 */
@Component
public class CreditCardStrategy implements PaymentStrategy {

    @Override
    public PaymentMethod method() {
        return PaymentMethod.CREDIT_CARD;
    }

    @Override
    public Set<String> requiredDetails() {
        return Set.of("cardToken");
    }

    @Override
    public Mono<ChargeResult> charge(ChargeRequest request) {
        return Mono.fromSupplier(() -> switch (request.details().get("cardToken")) {
            case "tok_declined" -> new Declined("CARD_DECLINED", "The card was declined");
            case "tok_insufficient_funds" -> new Declined("INSUFFICIENT_FUNDS", "The card has insufficient funds");
            default -> new Approved(SimulatedGateway.reference("cc_"), PaymentStatus.CAPTURED);
        });
    }

    @Override
    public Mono<RefundResult> refund(RefundRequest request) {
        return Mono.fromSupplier(() -> new RefundResult.Refunded(SimulatedGateway.reference("ccr_")));
    }
}
