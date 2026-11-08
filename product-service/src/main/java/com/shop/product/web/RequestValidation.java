package com.shop.product.web;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.shop.product.web.ProductDtos.CreateProductRequest;
import com.shop.product.web.ProductDtos.UpdateProductRequest;

/** Pure request checks: each returns the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_PAGE_SIZE = 100;

    private RequestValidation() {
    }

    static List<String> check(CreateProductRequest r) {
        List<String> errors = new ArrayList<>();
        text(errors, "sku", r.sku(), 64);
        text(errors, "name", r.name(), 200);
        price(errors, r.price());
        return errors;
    }

    static List<String> check(UpdateProductRequest r) {
        List<String> errors = new ArrayList<>();
        text(errors, "name", r.name(), 200);
        price(errors, r.price());
        if (r.active() == null) {
            errors.add("active is required");
        }
        if (r.version() == null || r.version() < 0) {
            errors.add("version is required and must be >= 0");
        }
        return errors;
    }

    private static void text(List<String> errors, String field, String value, int max) {
        if (value == null || value.isBlank()) {
            errors.add(field + " is required");
        } else if (value.length() > max) {
            errors.add(field + " must be at most " + max + " characters");
        }
    }

    private static void price(List<String> errors, BigDecimal price) {
        if (price == null) {
            errors.add("price is required");
        } else if (price.signum() < 0) {
            errors.add("price must be >= 0");
        } else if (price.scale() > 2) {
            errors.add("price must have at most 2 decimals");
        } else if (price.precision() - price.scale() > 10) {
            errors.add("price is too large");
        }
    }
}
