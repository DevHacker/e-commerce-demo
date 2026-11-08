package com.shop.product.domain;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Pure decision of the "ValidateProducts" saga step: given what the order asks for and what the
 * catalogue holds, either price every line or list every reason for refusal. No I/O.
 */
public final class ProductValidation {

    private ProductValidation() {
    }

    public enum Reason { NOT_FOUND, INACTIVE }

    public record PricedLine(UUID productId, BigDecimal unitPrice) {
    }

    public record Rejection(UUID productId, Reason reason) {
    }

    public sealed interface Result permits Validated, Rejected {
    }

    public record Validated(List<PricedLine> lines) implements Result {
    }

    public record Rejected(List<Rejection> rejections) implements Result {
    }

    /**
     * @param requested product ids of the order, duplicates allowed (reported once, in first-seen order)
     * @param catalogue products found, keyed by id; missing keys mean "unknown product"
     */
    public static Result validate(List<UUID> requested, Map<UUID, Product> catalogue) {
        Set<UUID> distinct = new LinkedHashSet<>(requested);
        List<PricedLine> priced = new ArrayList<>();
        List<Rejection> rejections = new ArrayList<>();
        for (UUID id : distinct) {
            Product product = catalogue.get(id);
            if (product == null) {
                rejections.add(new Rejection(id, Reason.NOT_FOUND));
            } else if (!product.active()) {
                rejections.add(new Rejection(id, Reason.INACTIVE));
            } else {
                priced.add(new PricedLine(id, product.price()));
            }
        }
        return rejections.isEmpty() ? new Validated(List.copyOf(priced)) : new Rejected(List.copyOf(rejections));
    }
}
