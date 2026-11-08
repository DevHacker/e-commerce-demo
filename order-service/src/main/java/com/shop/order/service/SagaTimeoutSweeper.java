package com.shop.order.service;

import java.time.Instant;

import com.shop.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Safety net of the saga: a saga that made no progress for {@code shop.order.saga-timeout} is rejected
 * (nothing reserved yet) or compensated (a reservation may exist). Each saga is handled in its own
 * transaction; a saga that moved in the meantime is skipped, and a lost optimistic race is simply left
 * for the next sweep. Safe to run on several instances.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OrderProperties.class)
@ConditionalOnProperty(prefix = "shop.order", name = "sweeper-enabled", havingValue = "true", matchIfMissing = true)
public class SagaTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

    private final OrderRepository repository;
    private final SagaCoordinator coordinator;
    private final TransactionalOperator transactions;
    private final OrderProperties properties;

    public SagaTimeoutSweeper(OrderRepository repository, SagaCoordinator coordinator,
                              TransactionalOperator transactions, OrderProperties properties) {
        this.repository = repository;
        this.coordinator = coordinator;
        this.transactions = transactions;
        this.properties = properties;
    }

    /**
     * Reactive {@code @Scheduled} method. Spring calls it once and re-subscribes to the returned Mono on
     * every run, so the cutoff is computed at subscription ({@code Mono.defer}), not at call time.
     */
    @Scheduled(fixedDelayString = "${shop.order.sweep-interval:30s}",
            initialDelayString = "${shop.order.sweep-interval:30s}")
    public Mono<Void> sweep() {
        return Mono.defer(() -> {
            Instant staleBefore = Instant.now().minus(properties.sagaTimeout());
            return repository.findStaleSagas(staleBefore, properties.sweepBatchSize())
                    .concatMap(id -> transactions.transactional(coordinator.timeout(id, staleBefore))
                            .doOnNext(applied -> {
                                if (applied) {
                                    log.warn("Saga {} timed out", id);
                                }
                            })
                            .onErrorResume(e -> {
                                log.info("Saga {} skipped this sweep: {}", id, e.toString());
                                return Mono.empty();
                            }))
                    .then();
        });
    }
}
