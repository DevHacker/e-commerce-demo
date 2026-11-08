package com.shop.inventory.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.shop.inventory.web.StockDtos.SetStockRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    @Test
    void acceptsValidRequests() {
        assertThat(RequestValidation.check(new SetStockRequest(0L, 1))).isEmpty();
        assertThat(RequestValidation.check(new SetStockRequest(1_000_000L, 256))).isEmpty();
    }

    @Test
    void listsEveryViolation() {
        assertThat(RequestValidation.check(new SetStockRequest(null, null))).containsExactly(
                "quantity is required and must be >= 0",
                "bucketCount is required and must be between 1 and 256");
        assertThat(RequestValidation.check(new SetStockRequest(-1L, 257))).hasSize(2);
        assertThat(RequestValidation.check(new SetStockRequest(5L, 0))).hasSize(1);
    }
}
