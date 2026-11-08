package com.shop.common.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * Reactive transaction support, kept apart from the connection configuration.
 * Business code wraps a {@code Mono}/{@code Flux} with
 * {@link TransactionalOperator#transactional(reactor.core.publisher.Mono)}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TransactionProperties.class)
public class TransactionConfig {

    @Bean
    public ReactiveTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }

    @Bean
    public TransactionalOperator transactionalOperator(ReactiveTransactionManager transactionManager,
                                                       TransactionProperties props) {
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        definition.setTimeout((int) props.timeout().toSeconds());
        return TransactionalOperator.create(transactionManager, definition);
    }
}
