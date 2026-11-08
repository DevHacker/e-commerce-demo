package com.shop.inventory.domain;

import java.util.List;
import java.util.UUID;

/** Result of trying to cover one order's needs from the buckets. */
public final class Allocation {

    private Allocation() {
    }

    /** {@code quantity} units taken from one bucket. */
    public record Take(UUID productId, int bucketNo, long quantity) {
    }

    /** What happened for one product: the takes made and what could not be covered. */
    public record ProductAllocation(UUID productId, long requested, List<Take> takes, long missing) {

        public boolean covered() {
            return missing == 0;
        }
    }

    public record Shortage(UUID productId, long requested, long missing) {
    }

    public sealed interface Outcome permits Reserved, Insufficient {
    }

    public record Reserved(List<Take> takes) implements Outcome {
    }

    public record Insufficient(List<Shortage> shortages) implements Outcome {
    }

    /** Pure decision: all products covered, or the list of every shortage. */
    public static Outcome decide(List<ProductAllocation> allocations) {
        List<Shortage> shortages = allocations.stream()
                .filter(a -> !a.covered())
                .map(a -> new Shortage(a.productId(), a.requested(), a.missing()))
                .toList();
        if (shortages.isEmpty()) {
            return new Reserved(allocations.stream().flatMap(a -> a.takes().stream()).toList());
        }
        return new Insufficient(shortages);
    }
}
