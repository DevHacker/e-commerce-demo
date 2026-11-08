package com.shop.product.domain;

import java.util.UUID;

public class ProductNotFoundException extends RuntimeException {

    public ProductNotFoundException(UUID id) {
        super("Product " + id + " not found");
    }
}
