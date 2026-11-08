package com.shop.common.messaging;

import java.util.UUID;

/**
 * JSON body of every saga message: correlation metadata plus the business {@code data}.
 * The message type travels separately (outbox column {@code type}, Kafka header {@code type}).
 */
public record Envelope<T>(UUID sagaId, UUID correlationId, String replyTo, T data) {

    public static <T> Envelope<T> of(UUID sagaId, UUID correlationId, String replyTo, T data) {
        return new Envelope<>(sagaId, correlationId, replyTo, data);
    }

    /** Same correlation metadata, different business data (typical for a reply). */
    public <R> Envelope<R> withData(R newData) {
        return new Envelope<>(sagaId, correlationId, replyTo, newData);
    }
}
