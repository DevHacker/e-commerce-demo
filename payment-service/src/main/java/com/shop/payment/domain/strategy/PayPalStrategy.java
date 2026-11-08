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

/** PayPal account payment. Simulation: a payer whose e-mail starts with {@code declined@} is refused. */
@Component
public class PayPalStrategy implements PaymentStrategy {

    @Override
    public PaymentMethod method() {
        return PaymentMethod.PAYPAL;
    }

    @Override
    public Set<String> requiredDetails() {
        return Set.of("payerEmail");
    }

    @Override
    public List<String> validate(Map<String, String> details) {
        List<String> problems = PaymentStrategy.super.validate(details);
        String email = details == null ? null : details.get("payerEmail");
        if (problems.isEmpty() && !email.contains("@")) {
            problems.add("payerEmail must be an e-mail address");
        }
        return problems;
    }

    @Override
    public Mono<ChargeResult> charge(ChargeRequest request) {
        return Mono.fromSupplier(() -> request.details().get("payerEmail").startsWith("declined@")
                ? new Declined("PAYER_DECLINED", "The PayPal payer declined the payment")
                : new Approved(SimulatedGateway.reference("pp_"), PaymentStatus.CAPTURED));
    }

    @Override
    public Mono<RefundResult> refund(RefundRequest request) {
        return Mono.fromSupplier(() -> new RefundResult.Refunded(SimulatedGateway.reference("ppr_")));
    }
}
