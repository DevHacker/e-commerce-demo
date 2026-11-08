package com.shop.product.domain;

import java.util.UUID;

/** The caller updated from a stale {@code version}: someone else changed the product first. */
public class VersionConflictException extends RuntimeException {

    public VersionConflictException(UUID id) {
        super("Product " + id + " was modified by someone else; reload and retry");
    }
}
