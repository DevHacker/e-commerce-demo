package com.shop.inventory.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Order in which an order's reservation visits the buckets of a product. Pure.
 *
 * <p>The first bucket is {@code hash(orderId) mod bucketCount}, so concurrent orders spread over
 * the buckets instead of fighting for the same row. The scan then goes up to the last bucket and
 * wraps to 0. Buckets are therefore locked in ascending order except at the wrap point; a deadlock
 * needs two orders to both spill over, which only happens when the product is nearly sold out.
 * PostgreSQL aborts one of them, the whole message transaction rolls back and the Kafka consumer
 * retries it.
 */
public final class BucketScan {

    private BucketScan() {
    }

    public static int startBucket(UUID orderId, int bucketCount) {
        return Math.floorMod(orderId.hashCode(), bucketCount);
    }

    public static List<Integer> order(UUID orderId, int bucketCount) {
        if (bucketCount <= 0) {
            return List.of();
        }
        int start = startBucket(orderId, bucketCount);
        List<Integer> scan = new ArrayList<>(bucketCount);
        for (int b = start; b < bucketCount; b++) {
            scan.add(b);
        }
        for (int b = 0; b < start; b++) {
            scan.add(b);
        }
        return List.copyOf(scan);
    }
}
