package com.shop.payment.domain.strategy;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.shop.payment.domain.PaymentMethod;

import org.springframework.stereotype.Component;

/**
 * The strategy lookup. Fails at startup, not at the first payment, when a method has no strategy or two
 * strategies claim the same method.
 */
@Component
public class PaymentStrategies {

    private final Map<PaymentMethod, PaymentStrategy> byMethod = new EnumMap<>(PaymentMethod.class);

    public PaymentStrategies(List<PaymentStrategy> strategies) {
        for (PaymentStrategy strategy : strategies) {
            PaymentStrategy previous = byMethod.put(strategy.method(), strategy);
            if (previous != null) {
                throw new IllegalStateException("Two strategies for " + strategy.method() + ": "
                        + previous.getClass().getSimpleName() + " and " + strategy.getClass().getSimpleName());
            }
        }
        for (PaymentMethod method : PaymentMethod.values()) {
            if (!byMethod.containsKey(method)) {
                throw new IllegalStateException("No strategy for " + method);
            }
        }
    }

    public Optional<PaymentStrategy> find(PaymentMethod method) {
        return Optional.ofNullable(byMethod.get(method));
    }

    public Set<PaymentMethod> supported() {
        return byMethod.keySet();
    }

    public Map<PaymentMethod, PaymentStrategy> all() {
        return Map.copyOf(byMethod);
    }
}
