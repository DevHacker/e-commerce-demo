package com.shop.order.service;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.outbox.OutboxWriter;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.SagaMessages;
import com.shop.order.domain.SagaMessages.Command;
import com.shop.order.domain.SagaMessages.EventType;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;

/** Writes the saga's outgoing messages (commands and domain events) to the outbox. Key = order id. */
@Component
public class SagaOutbox {

    static final String AGGREGATE_TYPE = "Order";

    /** Payload of the events published on {@code order.events}. */
    public record OrderEventData(UUID orderId, String customerRef, OrderStatus status, BigDecimal total,
                                 String reason) {
    }

    private final OutboxWriter outbox;

    public SagaOutbox(OutboxWriter outbox) {
        this.outbox = outbox;
    }

    public Mono<Void> command(UUID orderId, Command command) {
        Envelope<Object> envelope = Envelope.of(orderId, orderId, SagaMessages.SAGA_REPLIES, command.data());
        return outbox.write(command.topic(), AGGREGATE_TYPE, orderId.toString(), command.type(), envelope).then();
    }

    public Mono<Void> event(Order order, EventType type, OrderStatus status, String reason) {
        Envelope<OrderEventData> envelope = Envelope.of(order.id(),
                order.id(),
                null,
                new OrderEventData(order.id(),
                        order.customerRef(),
                        status,
                        order.total(),
                        reason)
        );

        return outbox.write(SagaMessages.ORDER_EVENTS,
                        AGGREGATE_TYPE,
                        order.id().toString(),
                        type.name(),
                        envelope)
                .then();
    }
}
