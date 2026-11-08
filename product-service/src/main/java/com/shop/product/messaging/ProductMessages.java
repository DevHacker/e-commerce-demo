package com.shop.product.messaging;

import java.util.List;
import java.util.UUID;

import com.shop.product.domain.ProductValidation;

/** Topics, message types and payloads (the {@code data} part of the envelope) of the Product service. */
public final class ProductMessages {

    public static final String COMMANDS_TOPIC = "product.commands";
    public static final String VALIDATE_PRODUCTS = "ValidateProducts";
    public static final String PRODUCTS_VALIDATED = "ProductsValidated";
    public static final String PRODUCTS_REJECTED = "ProductsRejected";

    private ProductMessages() {
    }

    public record ValidateProductsData(List<Item> items) {

        public record Item(UUID productId, int quantity) {
        }
    }

    public record ProductsValidatedData(List<ProductValidation.PricedLine> items) {
    }

    public record ProductsRejectedData(List<ProductValidation.Rejection> rejections) {
    }
}
