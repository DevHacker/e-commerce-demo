package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class StockSharesTest {

    @Test
    void splitsEvenly() {
        assertThat(StockShares.split(100, 4)).containsExactly(25, 25, 25, 25);
    }

    @Test
    void givesTheRemainderToTheFirstBuckets() {
        long[] shares = StockShares.split(10, 4);

        assertThat(shares).containsExactly(3, 3, 2, 2);
        assertThat(Arrays.stream(shares).sum()).isEqualTo(10);
    }

    @Test
    void handlesFewerUnitsThanBuckets() {
        assertThat(StockShares.split(2, 4)).containsExactly(1, 1, 0, 0);
        assertThat(StockShares.split(0, 3)).containsExactly(0, 0, 0);
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> StockShares.split(-1, 2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StockShares.split(5, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
