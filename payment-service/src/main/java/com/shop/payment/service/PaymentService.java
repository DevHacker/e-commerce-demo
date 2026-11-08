package com.shop.payment.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.shop.payment.domain.Payment;
import com.shop.payment.domain.PaymentMethod;
import com.shop.payment.domain.PaymentNotFoundException;
import com.shop.payment.domain.PaymentStatus;
import com.shop.payment.domain.strategy.ChargeRequest;
import com.shop.payment.domain.strategy.ChargeResult;
import com.shop.payment.domain.strategy.PaymentStrategies;
import com.shop.payment.domain.strategy.PaymentStrategy;
import com.shop.payment.domain.strategy.RefundRequest;
import com.shop.payment.domain.strategy.RefundResult;
import com.shop.payment.repository.PaymentRepository;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;

/**
 * Charges and refunds an order's payment. The method-specific work is delegated to the
 * {@link PaymentStrategy} for the order's {@link PaymentMethod}; this class has no method-specific code.
 * Both operations are idempotent per order, because commands are delivered at least once.
 */
@Service
public class PaymentService {

    /** The saga is told either {@link Completed} or {@link Failed}. */
    public sealed interface ChargeOutcome {
    }

    public record Completed(Payment payment) implements ChargeOutcome {
    }

    public record Failed(String code, String message) implements ChargeOutcome {
    }

    /** {@code refunded} is false when there was nothing to give back. {@code paymentId} is null if the order never had a payment. */
    public record RefundOutcome(boolean refunded, UUID paymentId) {
    }

    private final PaymentRepository repository;
    private final PaymentStrategies strategies;

    public PaymentService(PaymentRepository repository, PaymentStrategies strategies) {
        this.repository = repository;
        this.strategies = strategies;
    }

    public Mono<ChargeOutcome> charge(UUID orderId, String methodName, BigDecimal amount, Map<String, String> details) {
        return repository.findByOrder(orderId)
                .map(PaymentService::outcomeOf)
                .switchIfEmpty(Mono.defer(() -> attempt(orderId, methodName, amount, details)));
    }

    /** A replayed command gets the answer it got the first time, and nobody is charged twice. */
    private static ChargeOutcome outcomeOf(Payment existing) {
        return existing.status() == PaymentStatus.FAILED
                ? new Failed(existing.failureCode(), existing.failureMessage())
                : new Completed(existing);
    }

    private Mono<ChargeOutcome> attempt(UUID orderId, String methodName, BigDecimal amount, Map<String, String> details) {
        Optional<PaymentMethod> method = PaymentMethod.parse(methodName);
        if (method.isEmpty()) {
            return fail(orderId, methodName, amount, "UNSUPPORTED_METHOD", "Unsupported payment method: " + methodName);
        }
        if (amount == null || amount.signum() <= 0) {
            return fail(orderId, methodName, amount, "INVALID_AMOUNT", "The amount must be positive");
        }
        PaymentStrategy strategy = strategies.find(method.get()).orElseThrow();
        List<String> problems = strategy.validate(details);
        if (!problems.isEmpty()) {
            return fail(orderId, method.get().name(), amount, "INVALID_DETAILS", String.join("; ", problems));
        }
        return strategy.charge(new ChargeRequest(orderId, amount, details))
                .flatMap(result -> switch (result) {
                    case ChargeResult.Approved ok -> {
                        Payment payment = new Payment(UUID.randomUUID(), orderId, method.get().name(), amount,
                                ok.status(), ok.reference(), null, null, 0);
                        yield repository.insert(payment).thenReturn((ChargeOutcome) new Completed(payment));
                    }
                    case ChargeResult.Declined ko -> fail(orderId, method.get().name(), amount, ko.code(), ko.message());
                });
    }

    private Mono<ChargeOutcome> fail(UUID orderId, String method, BigDecimal amount, String code, String message) {
        String storedMethod = method == null ? "UNKNOWN" : method.trim().substring(0, Math.min(method.trim().length(), 32));
        Payment failed = new Payment(UUID.randomUUID(), orderId, storedMethod,
                amount == null ? BigDecimal.ZERO : amount, PaymentStatus.FAILED, null, code, message, 0);
        return repository.insert(failed).thenReturn(new Failed(code, message));
    }

    public Mono<RefundOutcome> refund(UUID orderId) {
        return repository.findByOrder(orderId)
                .flatMap(this::refund)
                .defaultIfEmpty(new RefundOutcome(false, null));
    }

    private Mono<RefundOutcome> refund(Payment payment) {
        if (!payment.status().refundable()) {
            return Mono.just(new RefundOutcome(false, payment.id())); // failed, or already refunded or voided
        }
        PaymentStrategy strategy = strategies.find(PaymentMethod.parse(payment.method()).orElseThrow()).orElseThrow();
        return strategy.refund(new RefundRequest(payment.reference(), payment.amount(), payment.status()))
                .flatMap(result -> switch (result) {
                    case RefundResult.Refunded r -> repository.updateStatus(payment, PaymentStatus.REFUNDED)
                            .thenReturn(new RefundOutcome(true, payment.id()));
                    case RefundResult.Voided v -> repository.updateStatus(payment, PaymentStatus.VOIDED)
                            .thenReturn(new RefundOutcome(false, payment.id()));
                });
    }

    public Mono<Payment> get(UUID orderId) {
        return repository.findByOrder(orderId).switchIfEmpty(Mono.error(() -> new PaymentNotFoundException(orderId)));
    }
}
