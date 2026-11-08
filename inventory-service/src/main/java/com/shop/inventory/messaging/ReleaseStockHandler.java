package com.shop.inventory.messaging;

import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.inventory.messaging.InventoryMessages.ReleaseStockData;
import com.shop.inventory.messaging.InventoryMessages.StockReleasedData;
import com.shop.inventory.service.ReservationService;
import reactor.core.publisher.Mono;

/**
 * Saga compensation "ReleaseStock": gives back exactly what the order reserved. An order with nothing
 * reserved is a no-op that still answers {@code StockReleased}, so the saga can always finish.
 */
public class ReleaseStockHandler implements Handler {

    private final ReservationService reservations;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ReleaseStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        this.reservations = reservations;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ReleaseStockData> command = message.envelope(codec, ReleaseStockData.class);
        if (command.sagaId() == null) {
            throw new NonRetryableException("ReleaseStock without sagaId");
        }
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ReleaseStock without replyTo");
        }
        UUID orderId = command.sagaId();
        String key = message.key() != null ? message.key() : orderId.toString();

        return reservations.release(orderId)
                .flatMap(released -> outbox.write(command.replyTo(), ReserveStockHandler.AGGREGATE_TYPE, key,
                        InventoryMessages.STOCK_RELEASED, command.withData(new StockReleasedData(released))))
                .then();
    }
}
