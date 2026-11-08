package com.shop.inventory.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.outbox.OutboxWriter;
import com.shop.inventory.service.ReservationService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: which handler answers which command type on {@code inventory.commands}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class InventoryMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "inventory-service." + InventoryMessages.COMMANDS_TOPIC;

    @Bean
    public ReserveStockHandler reserveStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        return new ReserveStockHandler(reservations, outbox, codec);
    }

    @Bean
    public ReleaseStockHandler releaseStockHandler(ReservationService reservations, OutboxWriter outbox, JsonCodec codec) {
        return new ReleaseStockHandler(reservations, outbox, codec);
    }

    @Bean
    public HandlerRegistry inventoryCommandHandlers(ReserveStockHandler reserve, ReleaseStockHandler release) {
        return HandlerRegistry.builder()
                .on(InventoryMessages.RESERVE_STOCK, reserve)
                .on(InventoryMessages.RELEASE_STOCK, release)
                .build();
    }

    @Bean
    public MessageDispatcher inventoryCommandsDispatcher(
            HandlerRegistry inventoryCommandHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(
                CONSUMER_NAME, inventoryCommandHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
