package com.shop.order.domain;

import java.util.UUID;

public final class OrderExceptions {

    private OrderExceptions() {
    }

    public static class OrderNotFoundException extends RuntimeException {
        public OrderNotFoundException(UUID id) {
            super("Order " + id + " not found");
        }
    }

    /** The request contradicts the current state of the order (for example cancelling an unconfirmed order). */
    public static class OrderConflictException extends RuntimeException {
        public OrderConflictException(String message) {
            super(message);
        }
    }

    /**
     * Someone else changed the saga or the order between our read and our write. Transient: the whole
     * message transaction rolls back and the Kafka consumer replays it, or the caller retries.
     */
    public static class ConcurrentUpdateException extends RuntimeException {
        public ConcurrentUpdateException(String what, UUID id) {
            super(what + " " + id + " was modified concurrently");
        }
    }
}
