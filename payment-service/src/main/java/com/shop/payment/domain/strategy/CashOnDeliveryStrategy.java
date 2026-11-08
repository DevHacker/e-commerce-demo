package com.shop.payment.domain.strategy;

import java.math.BigDecimal;
import java.util.Set;

import com.shop.payment.domain.PaymentMethod;
import com.shop.payment.domain.PaymentStatus;
import com.shop.payment.domain.strategy.ChargeResult.Approved;
import com.shop.payment.domain.strategy.ChargeResult.Declined;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;

/**
 * Cash is collected by the courier, so nothing is charged now: the payment is accepted and left
 * {@code PENDING_ON_DELIVERY}. Orders above {@link #LIMIT} cannot be paid this way. A refund of a payment
 * nobody collected is a void: there is no money to give back.
 */
@Component
public class CashOnDeliveryStrategy implements PaymentStrategy {

    static final BigDecimal LIMIT = new BigDecimal("500.00");

    @Override
    public PaymentMethod method() {
        return PaymentMethod.CASH_ON_DELIVERY;
    }

    @Override
    public Set<String> requiredDetails() {
        return Set.of("deliveryAddress");
    }

    @Override
    public Mono<ChargeResult> charge(ChargeRequest request) {
        return Mono.fromSupplier(() -> request.amount().compareTo(LIMIT) > 0
                ? new Declined("COD_LIMIT_EXCEEDED", "Cash on delivery is limited to " + LIMIT)
                : new Approved(SimulatedGateway.reference("cod_"), PaymentStatus.PENDING_ON_DELIVERY));
    }

    @Override
    public Mono<RefundResult> refund(RefundRequest request) {
        return Mono.just(new RefundResult.Voided());
    }
}
