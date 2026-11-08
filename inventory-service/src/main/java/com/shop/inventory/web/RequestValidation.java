package com.shop.inventory.web;

import java.util.ArrayList;
import java.util.List;

import com.shop.inventory.web.StockDtos.SetStockRequest;

/** Pure request checks: the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_BUCKETS = 256;

    private RequestValidation() {
    }

    static List<String> check(SetStockRequest request) {
        List<String> errors = new ArrayList<>();
        if (request.quantity() == null || request.quantity() < 0) {
            errors.add("quantity is required and must be >= 0");
        }
        if (request.bucketCount() == null || request.bucketCount() < 1 || request.bucketCount() > MAX_BUCKETS) {
            errors.add("bucketCount is required and must be between 1 and " + MAX_BUCKETS);
        }
        return errors;
    }
}
