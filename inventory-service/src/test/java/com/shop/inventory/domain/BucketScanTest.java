package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class BucketScanTest {

    @Test
    void visitsEveryBucketExactlyOnce() {
        UUID orderId = UUID.randomUUID();

        List<Integer> scan = BucketScan.order(orderId, 8);

        assertThat(scan).hasSize(8).doesNotHaveDuplicates().containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void startsAtTheHashBucketGoesUpThenWrapsToZero() {
        UUID orderId = new UUID(0, 5); // hashCode() == 5 for (0, 5)
        assertThat(orderId.hashCode()).isEqualTo(5);

        assertThat(BucketScan.order(orderId, 8)).containsExactly(5, 6, 7, 0, 1, 2, 3, 4);
        assertThat(BucketScan.order(orderId, 4)).containsExactly(1, 2, 3, 0); // 5 mod 4 = 1
    }

    @Test
    void aSingleBucketAlwaysScansBucketZero() {
        assertThat(BucketScan.order(UUID.randomUUID(), 1)).containsExactly(0);
    }

    @Test
    void noBucketsMeansNothingToScan() {
        assertThat(BucketScan.order(UUID.randomUUID(), 0)).isEmpty();
    }

    @Test
    void negativeHashCodesStayInRange() {
        UUID negative = new UUID(0, 0xFFFFFFFFL);
        assertThat(negative.hashCode()).isNegative();
        assertThat(BucketScan.startBucket(negative, 16)).isBetween(0, 15);
    }

    @Test
    void ordersSpreadAcrossAllStartBuckets() {
        Set<Integer> starts = new HashSet<>();
        IntStream.range(0, 2_000).forEach(i -> starts.add(BucketScan.startBucket(UUID.randomUUID(), 16)));

        assertThat(starts).hasSize(16);
    }
}
