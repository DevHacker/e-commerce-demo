package com.shop.order.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.order.service.SagaCoordinator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: the replies of Product and Inventory, handled by the saga coordinator. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class OrderMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "order-service." + ReplyMessages.TOPIC;

    @Bean
    public HandlerRegistry sagaReplyHandlers(SagaCoordinator coordinator, JsonCodec codec) {
        return SagaReplyHandlers.registry(coordinator, codec);
    }

    @Bean
    public MessageDispatcher sagaRepliesDispatcher(
            HandlerRegistry sagaReplyHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(CONSUMER_NAME, sagaReplyHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
