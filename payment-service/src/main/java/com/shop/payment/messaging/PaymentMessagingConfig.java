package com.shop.payment.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.outbox.OutboxWriter;
import com.shop.payment.service.PaymentService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Wires the consumer side: which handler answers which command type on {@code payment.commands}. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class PaymentMessagingConfig {

    /** Inbox key, convention {@code <service>.<topic>}. */
    static final String CONSUMER_NAME = "payment-service." + PaymentMessages.COMMANDS_TOPIC;

    @Bean
    public ProcessPaymentHandler processPaymentHandler(PaymentService payments, OutboxWriter outbox, JsonCodec codec) {
        return new ProcessPaymentHandler(payments, outbox, codec);
    }

    @Bean
    public RefundPaymentHandler refundPaymentHandler(PaymentService payments, OutboxWriter outbox, JsonCodec codec) {
        return new RefundPaymentHandler(payments, outbox, codec);
    }

    @Bean
    public HandlerRegistry paymentCommandHandlers(ProcessPaymentHandler process, RefundPaymentHandler refund) {
        return HandlerRegistry.builder()
                .on(PaymentMessages.PROCESS_PAYMENT, process)
                .on(PaymentMessages.REFUND_PAYMENT, refund)
                .build();
    }

    @Bean
    public MessageDispatcher paymentCommandsDispatcher(
            HandlerRegistry paymentCommandHandlers, Inbox inbox, TransactionalOperator transactions) {
        return new MessageDispatcher(
                CONSUMER_NAME, paymentCommandHandlers, inbox, transactions, Duration.ofSeconds(10));
    }
}
