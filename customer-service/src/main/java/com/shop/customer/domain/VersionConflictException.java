package com.shop.customer.domain;

import java.util.UUID;

/** The caller updated from a stale {@code version}: someone else changed the customer first. */
public class VersionConflictException extends RuntimeException {

    public VersionConflictException(UUID id) {
        super("Customer " + id + " was modified by someone else; reload and retry");
    }
}
