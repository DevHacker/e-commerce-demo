package com.shop.common.messaging;

import java.util.function.Function;

import reactor.core.publisher.Mono;

/**
 * Business reaction to one message type. Everything it does (database writes, outbox rows)
 * happens inside the {@code Mono}, which the dispatcher runs in a single transaction.
 */
@FunctionalInterface
public interface Handler extends Function<InboundMessage, Mono<Void>> {
}
