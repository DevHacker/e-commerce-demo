package com.shop.product.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import com.shop.product.web.ProductDtos.CreateProductRequest;
import com.shop.product.web.ProductDtos.UpdateProductRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    @Test
    void acceptsAValidCreateRequest() {
        assertThat(RequestValidation.check(new CreateProductRequest("SKU-1", "Mug", new BigDecimal("9.99"))))
                .isEmpty();
    }

    @Test
    void listsEveryViolationOfACreateRequest() {
        assertThat(RequestValidation.check(new CreateProductRequest(" ", null, new BigDecimal("-1"))))
                .containsExactly("sku is required", "name is required", "price must be >= 0");
    }

    @Test
    void rejectsMoreThanTwoDecimalsAndHugePrices() {
        assertThat(RequestValidation.check(new CreateProductRequest("S", "N", new BigDecimal("1.005"))))
                .containsExactly("price must have at most 2 decimals");
        assertThat(RequestValidation.check(new CreateProductRequest("S", "N", new BigDecimal("12345678901"))))
                .containsExactly("price is too large");
    }

    @Test
    void rejectsTooLongSku() {
        assertThat(RequestValidation.check(new CreateProductRequest("x".repeat(65), "N", BigDecimal.ONE)))
                .containsExactly("sku must be at most 64 characters");
    }

    @Test
    void updateRequiresActiveAndVersion() {
        assertThat(RequestValidation.check(new UpdateProductRequest("Mug", BigDecimal.ONE, null, null)))
                .containsExactly("active is required", "version is required and must be >= 0");
        assertThat(RequestValidation.check(new UpdateProductRequest("Mug", BigDecimal.ONE, true, 3L))).isEmpty();
    }
}
