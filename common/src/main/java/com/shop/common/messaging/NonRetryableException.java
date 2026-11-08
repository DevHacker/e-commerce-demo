package com.shop.common.messaging;

/**
 * Thrown for failures that retrying cannot fix (unreadable payload, unknown message type, missing
 * header). The Kafka error handler sends the record straight to the dead letter topic.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
