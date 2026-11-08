package com.shop.inventory.messaging;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.Outcome;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.domain.StockNeeds;
import com.shop.inventory.messaging.InventoryMessages.ReserveStockData;
import com.shop.inventory.messaging.InventoryMessages.StockRejectedData;
import com.shop.inventory.messaging.InventoryMessages.StockReservedData;
import com.shop.inventory.service.ReservationService;
import reactor.core.publisher.Mono;

/**
 * Saga step "ReserveStock". Business refusal (not enough stock) is an answer, never an exception: the
 * reservation service leaves the stock untouched and this handler writes {@code StockRejected}. Runs
 * inside the dispatcher's transaction, so inbox row, stock changes and outbox reply commit together.
 * The order id is the saga id (spec: {@code saga_id = order_id}).
 */
public class ReserveStockHandler implements Handler {

    static final String AGGREGATE_TYPE = "Order";

    private final ReservationService reservations;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ReserveStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        this.reservations = reservations;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ReserveStockData> command = message.envelope(codec, ReserveStockData.class);
        UUID orderId = command.sagaId();
        Map<UUID, Long> needs = needs(command);
        String key = message.key() != null ? message.key() : orderId.toString();

        return reservations.reserve(orderId, needs)
                .flatMap(outcome -> reply(key, command, outcome))
                .then();
    }

    private Mono<UUID> reply(String key, Envelope<ReserveStockData> command, Outcome outcome) {
        return switch (outcome) {
            case Reserved ok -> outbox.write(command.replyTo(), AGGREGATE_TYPE, key,
                    InventoryMessages.STOCK_RESERVED, command.withData(new StockReservedData(ok.takes())));
            case Insufficient ko -> outbox.write(command.replyTo(), AGGREGATE_TYPE, key,
                    InventoryMessages.STOCK_REJECTED, command.withData(new StockRejectedData(ko.shortages())));
        };
    }

    private static Map<UUID, Long> needs(Envelope<ReserveStockData> command) {
        if (command.sagaId() == null) {
            throw new NonRetryableException("ReserveStock without sagaId");
        }
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ReserveStock without replyTo");
        }
        ReserveStockData data = command.data();
        if (data == null || data.items() == null || data.items().isEmpty()) {
            throw new NonRetryableException("ReserveStock without items");
        }
        try {
            List<StockNeeds.Line> lines = data.items().stream()
                    .map(i -> new StockNeeds.Line(i.productId(), i.quantity()))
                    .toList();
            return StockNeeds.merge(lines);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableException(e.getMessage(), e);
        }
    }
}
