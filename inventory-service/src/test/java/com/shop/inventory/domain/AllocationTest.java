package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.ProductAllocation;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.domain.Allocation.Shortage;
import com.shop.inventory.domain.Allocation.Take;
import org.junit.jupiter.api.Test;

class AllocationTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    @Test
    void reservesWhenEveryProductIsCovered() {
        var a = new ProductAllocation(A, 5, List.of(new Take(A, 0, 3), new Take(A, 1, 2)), 0);
        var b = new ProductAllocation(B, 1, List.of(new Take(B, 0, 1)), 0);

        assertThat(Allocation.decide(List.of(a, b))).isEqualTo(new Reserved(List.of(
                new Take(A, 0, 3), new Take(A, 1, 2), new Take(B, 0, 1))));
    }

    @Test
    void reportsEveryShortageAndNoTakes() {
        var a = new ProductAllocation(A, 5, List.of(new Take(A, 0, 3)), 2);
        var b = new ProductAllocation(B, 1, List.of(new Take(B, 0, 1)), 0);
        var c = new ProductAllocation(UUID.randomUUID(), 4, List.of(), 4);

        var outcome = Allocation.decide(List.of(a, b, c));

        assertThat(outcome).isInstanceOf(Insufficient.class);
        assertThat(((Insufficient) outcome).shortages()).containsExactly(
                new Shortage(A, 5, 2), new Shortage(c.productId(), 4, 4));
    }
}
