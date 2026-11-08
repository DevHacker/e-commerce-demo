package com.shop.product.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.outbox.OutboxWriter;
import com.shop.product.repository.ProductRepository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: which handler answers which command type on {@code product.commands}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class ProductMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "product-service." + ProductMessages.COMMANDS_TOPIC;

    @Bean
    public ValidateProductsHandler validateProductsHandler(
            ProductRepository products, OutboxWriter outbox, JsonCodec codec) {
        return new ValidateProductsHandler(products, outbox, codec);
    }

    @Bean
    public HandlerRegistry productCommandHandlers(ValidateProductsHandler validateProducts) {
        return HandlerRegistry.builder()
                .on(ProductMessages.VALIDATE_PRODUCTS, validateProducts)
                .build();
    }

    @Bean
    public MessageDispatcher productCommandsDispatcher(
            HandlerRegistry productCommandHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(
                CONSUMER_NAME, productCommandHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
