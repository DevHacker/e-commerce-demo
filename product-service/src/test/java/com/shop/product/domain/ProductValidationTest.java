package com.shop.product.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.product.domain.ProductValidation.PricedLine;
import com.shop.product.domain.ProductValidation.Reason;
import com.shop.product.domain.ProductValidation.Rejected;
import com.shop.product.domain.ProductValidation.Rejection;
import com.shop.product.domain.ProductValidation.Validated;
import org.junit.jupiter.api.Test;

class ProductValidationTest {

    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();
    private static final UUID MISSING = UUID.randomUUID();

    private static Product product(UUID id, String price, boolean active) {
        return new Product(id, "sku-" + id, "name", new BigDecimal(price), active, 0);
    }

    @Test
    void pricesEveryLineWhenAllProductsAreActive() {
        var catalogue = Map.of(A, product(A, "10.50", true), B, product(B, "3.00", true));

        var result = ProductValidation.validate(List.of(A, B), catalogue);

        assertThat(result).isEqualTo(new Validated(List.of(
                new PricedLine(A, new BigDecimal("10.50")),
                new PricedLine(B, new BigDecimal("3.00")))));
    }

    @Test
    void rejectsUnknownProduct() {
        var result = ProductValidation.validate(List.of(MISSING), Map.of());

        assertThat(result).isEqualTo(new Rejected(List.of(new Rejection(MISSING, Reason.NOT_FOUND))));
    }

    @Test
    void rejectsInactiveProduct() {
        var result = ProductValidation.validate(List.of(A), Map.of(A, product(A, "1.00", false)));

        assertThat(result).isEqualTo(new Rejected(List.of(new Rejection(A, Reason.INACTIVE))));
    }

    @Test
    void reportsEveryRejectionNotOnlyTheFirst() {
        var catalogue = Map.of(A, product(A, "1.00", false), B, product(B, "2.00", true));

        var result = ProductValidation.validate(List.of(A, B, MISSING), catalogue);

        assertThat(result).isEqualTo(new Rejected(List.of(
                new Rejection(A, Reason.INACTIVE),
                new Rejection(MISSING, Reason.NOT_FOUND))));
    }

    @Test
    void reportsADuplicatedProductOnceInFirstSeenOrder() {
        var catalogue = Map.of(A, product(A, "1.00", true), B, product(B, "2.00", true));

        var result = ProductValidation.validate(List.of(B, A, B), catalogue);

        assertThat(result).isEqualTo(new Validated(List.of(
                new PricedLine(B, new BigDecimal("2.00")),
                new PricedLine(A, new BigDecimal("1.00")))));
    }
}
