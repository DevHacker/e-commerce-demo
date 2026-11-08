package com.shop.inventory.domain;

/** The requested stock change contradicts reservations that are still active. */
public class StockConflictException extends RuntimeException {

    public StockConflictException(String message) {
        super(message);
    }
}
