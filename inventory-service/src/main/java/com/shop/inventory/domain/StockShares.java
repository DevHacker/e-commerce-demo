package com.shop.inventory.domain;

/** Splits a quantity over buckets as evenly as possible; the remainder goes to the first buckets. Pure. */
public final class StockShares {

    private StockShares() {
    }

    public static long[] split(long quantity, int bucketCount) {
        if (quantity < 0 || bucketCount <= 0) {
            throw new IllegalArgumentException("quantity >= 0 and bucketCount > 0 required");
        }
        long[] shares = new long[bucketCount];
        long base = quantity / bucketCount;
        long remainder = quantity % bucketCount;
        for (int i = 0; i < bucketCount; i++) {
            shares[i] = base + (i < remainder ? 1 : 0);
        }
        return shares;
    }
}
