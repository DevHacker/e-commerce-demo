package com.shop.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.domain.Records.OrderItem;
import org.junit.jupiter.api.Test;

class PricingTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    @Test
    void totalIsTheSumOfQuantityTimesUnitPrice() {
        var items = List.of(new OrderItem(A, 2, null), new OrderItem(B, 3, null));

        var pricing = Pricing.of(items, Map.of(A, new BigDecimal("10.50"), B, new BigDecimal("1.20"))).orElseThrow();

        assertThat(pricing.total()).isEqualByComparingTo("24.60");
        assertThat(pricing.unitPrices()).containsEntry(A, new BigDecimal("10.50"));
    }

    @Test
    void isEmptyWhenAnItemHasNoPrice() {
        var items = List.of(new OrderItem(A, 1, null), new OrderItem(B, 1, null));

        assertThat(Pricing.of(items, Map.of(A, BigDecimal.ONE))).isEmpty();
    }
}
