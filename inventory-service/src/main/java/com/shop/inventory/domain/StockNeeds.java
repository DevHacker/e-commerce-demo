package com.shop.inventory.domain;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The quantities an order asks for, one entry per product, sorted by product id. Pure.
 * Sorting makes every order lock products in the same sequence, which rules out deadlocks between
 * products.
 */
public final class StockNeeds {

    private StockNeeds() {
    }

    public record Line(UUID productId, long quantity) {
    }

    /** @throws IllegalArgumentException if a quantity is not positive (a malformed command, not retryable) */
    public static Map<UUID, Long> merge(List<Line> lines) {
        Map<UUID, Long> needs = new TreeMap<>();
        for (Line line : lines) {
            if (line.quantity() <= 0) {
                throw new IllegalArgumentException(
                        "Quantity must be positive for product " + line.productId() + ": " + line.quantity());
            }
            needs.merge(line.productId(), line.quantity(), Long::sum);
        }
        return needs;
    }
}
