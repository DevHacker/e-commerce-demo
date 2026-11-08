package com.shop.order.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.shop.order.domain.Records.OrderItem;

/** Price snapshot of an order: the unit price of every item and the total. Pure. */
public record Pricing(Map<UUID, BigDecimal> unitPrices, BigDecimal total) {

    /** Empty if some item has no price in {@code prices} (the Product service should never do that). */
    public static Optional<Pricing> of(List<OrderItem> items, Map<UUID, BigDecimal> prices) {
        BigDecimal total = BigDecimal.ZERO;
        for (OrderItem item : items) {
            BigDecimal price = prices.get(item.productId());
            if (price == null) {
                return Optional.empty();
            }
            total = total.add(price.multiply(BigDecimal.valueOf(item.quantity())));
        }
        return Optional.of(new Pricing(Map.copyOf(prices), total));
    }
}
