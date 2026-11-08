package com.shop.payment.domain.strategy;

import java.util.UUID;

/** The part of a gateway the simulated strategies share: gateway-style references. */
final class SimulatedGateway {

    private SimulatedGateway() {
    }

    static String reference(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
    }
}
