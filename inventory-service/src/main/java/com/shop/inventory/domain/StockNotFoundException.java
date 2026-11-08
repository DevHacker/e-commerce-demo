package com.shop.inventory.domain;

import java.util.UUID;

public class StockNotFoundException extends RuntimeException {

    public StockNotFoundException(UUID productId) {
        super("No stock defined for product " + productId);
    }
}
