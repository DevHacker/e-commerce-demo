package com.shop.inventory.messaging;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.domain.Allocation;

/** Topics, message types and payloads (the {@code data} part of the envelope) of the Inventory service. */
public final class InventoryMessages {

    public static final String COMMANDS_TOPIC = "inventory.commands";
    public static final String RESERVE_STOCK = "ReserveStock";
    public static final String RELEASE_STOCK = "ReleaseStock";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";
    public static final String STOCK_RELEASED = "StockReleased";

    private InventoryMessages() {
    }

    public record ReserveStockData(List<Item> items) {

        public record Item(UUID productId, long quantity) {
        }
    }

    /** {@code reason} is informational (for example {@code ORDER_CANCELLED}); it may be absent. */
    public record ReleaseStockData(String reason) {
    }

    public record StockReservedData(List<Allocation.Take> reservations) {
    }

    public record StockRejectedData(List<Allocation.Shortage> shortages) {
    }

    public record StockReleasedData(List<Allocation.Take> released) {
    }
}
