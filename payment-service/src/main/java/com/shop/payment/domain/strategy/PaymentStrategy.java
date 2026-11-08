package com.shop.payment.domain.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.shop.payment.domain.PaymentMethod;
import reactor.core.publisher.Mono;

/**
 * Strategy pattern: one implementation per payment method, chosen at run time by {@link PaymentMethod}.
 * The service never branches on the method; it asks {@link PaymentStrategies} for the strategy and calls it.
 * A strategy owns everything method-specific: which details it needs, how they are checked, what its
 * gateway does on charge and on refund.
 *
 * <p>Charge and refund return a {@code Mono}: a real gateway is a network call. The implementations here
 * simulate their gateway (no credentials, no network); replacing one by the real SDK client changes that
 * class only.
 */
public interface PaymentStrategy {

    PaymentMethod method();

    /** The detail fields this method needs. */
    Set<String> requiredDetails();

    /** Problems with the details, empty when they are usable. Pure; override to add format rules. */
    default List<String> validate(Map<String, String> details) {
        List<String> problems = new ArrayList<>();
        for (String name : requiredDetails()) {
            String value = details == null ? null : details.get(name);
            if (value == null || value.isBlank()) {
                problems.add(name + " is required for " + method());
            }
        }
        return problems;
    }

    Mono<ChargeResult> charge(ChargeRequest request);

    Mono<RefundResult> refund(RefundRequest request);
}
