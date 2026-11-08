package com.shop.product.web;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.shop.product.domain.Product;

/** Wire format of the HTTP API. */
public final class ProductDtos {

    private ProductDtos() {
    }

    public record CreateProductRequest(String sku, String name, BigDecimal price) {
    }

    public record UpdateProductRequest(String name, BigDecimal price, Boolean active, Long version) {
    }

    public record ProductResponse(UUID id, String sku, String name, BigDecimal price, boolean active, long version) {

        public static ProductResponse from(Product p) {
            return new ProductResponse(p.id(), p.sku(), p.name(), p.price(), p.active(), p.version());
        }
    }

    public record PageResponse<T>(List<T> items, int page, int size, boolean hasNext) {
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
