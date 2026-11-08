package com.shop.product.domain;

import java.math.BigDecimal;
import java.util.UUID;

public record Product(UUID id, String sku, String name, BigDecimal price, boolean active, long version) {
}
