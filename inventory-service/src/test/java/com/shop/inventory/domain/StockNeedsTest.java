package com.shop.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.domain.StockNeeds.Line;
import org.junit.jupiter.api.Test;

class StockNeedsTest {

    private static final UUID LOW = new UUID(0, 1);
    private static final UUID HIGH = new UUID(0, 2);

    @Test
    void sumsDuplicatedProducts() {
        var needs = StockNeeds.merge(List.of(new Line(LOW, 2), new Line(LOW, 3)));

        assertThat(needs).containsExactly(java.util.Map.entry(LOW, 5L));
    }

    @Test
    void sortsByProductIdWhateverTheInputOrder() {
        var needs = StockNeeds.merge(List.of(new Line(HIGH, 1), new Line(LOW, 1)));

        assertThat(needs.keySet()).containsExactly(LOW, HIGH);
    }

    @Test
    void rejectsNonPositiveQuantities() {
        assertThatThrownBy(() -> StockNeeds.merge(List.of(new Line(LOW, 0))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StockNeeds.merge(List.of(new Line(LOW, -4))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
