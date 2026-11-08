package com.shop.order.service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.domain.OrderExceptions.ConcurrentUpdateException;
import com.shop.order.domain.OrderExceptions.OrderNotFoundException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.SagaMachine.Direction;
import com.shop.order.domain.SagaMachine.StepEntry;
import com.shop.order.domain.SagaMessages;
import com.shop.order.domain.SagaMessages.EventType;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.domain.SagaMessages.ValidateProducts;
import com.shop.order.domain.SagaState;
import com.shop.order.repository.OrderRepository;
import com.shop.order.repository.OrderRepository.StatusHistoryRow;
import com.shop.order.repository.OrderRepository.StepHistoryRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Use cases behind the HTTP API.
 */
@Service
public class OrderService {

    public record Created(UUID orderId, OrderStatus status) {
    }

    public record History(List<StatusHistoryRow> status, List<StepHistoryRow> steps) {
    }

    private final OrderRepository repository;
    private final SagaOutbox outbox;
    private final SagaCoordinator coordinator;
    private final TransactionalOperator transactions;

    public OrderService(OrderRepository repository, SagaOutbox outbox, SagaCoordinator coordinator,
                        TransactionalOperator transactions) {
        this.repository = repository;
        this.outbox = outbox;
        this.coordinator = coordinator;
        this.transactions = transactions;
    }

    /**
     * Creates the order and starts the saga in ONE transaction: order, items, saga state, both
     * histories, the ValidateProducts command and the OrderCreated event. The same idempotency key
     * always returns the same order, also when two identical requests race.
     */
    public Mono<Created> create(String idempotencyKey, String customerRef, List<Line> items,
                                String paymentMethod, Map<String, String> paymentDetails) {
        return existing(idempotencyKey)
                .switchIfEmpty(Mono.defer(() -> transactions.transactional(
                                start(idempotencyKey,
                                        customerRef,
                                        items,
                                        paymentMethod,
                                        paymentDetails)
                        )).onErrorResume(DuplicateKeyException.class, e -> existing(idempotencyKey)));
    }

    private Mono<Created> existing(String idempotencyKey) {
        return repository.findOrderByIdempotencyKey(idempotencyKey).map(o -> new Created(o.id(), o.status()));
    }

    private Mono<Created> start(String idempotencyKey, String customerRef, List<Line> items,
                                String paymentMethod, Map<String, String> paymentDetails) {
        UUID orderId = UUID.randomUUID();
        Order order = new Order(orderId, customerRef, OrderStatus.PENDING, null, paymentMethod, paymentDetails,
                idempotencyKey, 0);
        return repository.insertOrder(orderId, customerRef, paymentMethod, paymentDetails, idempotencyKey)
                .thenMany(Flux.fromIterable(items).concatMap(i -> repository.insertItem(orderId, i.productId(), i.quantity())))
                .then(repository.insertSaga(orderId, SagaState.STARTED, SagaMessages.STEP_VALIDATE_PRODUCTS))
                .then(repository.insertStatusHistory(orderId, null, OrderStatus.PENDING, "ORDER_CREATED", null))
                .then(repository.insertStepHistory(orderId,
                        new StepEntry(SagaMessages.STEP_VALIDATE_PRODUCTS, Direction.FORWARD, "REQUESTED", null), null, null))
                .then(outbox.command(orderId, new ValidateProducts(items)))
                .then(outbox.event(order, EventType.OrderCreated, OrderStatus.PENDING, "ORDER_CREATED"))
                .thenReturn(new Created(orderId, OrderStatus.PENDING));
    }

    public Mono<Loaded> get(UUID orderId) {
        return repository.load(orderId).switchIfEmpty(Mono.error(() -> new OrderNotFoundException(orderId)));
    }

    public Mono<History> history(UUID orderId) {
        return repository.findOrder(orderId)
                .switchIfEmpty(Mono.error(() -> new OrderNotFoundException(orderId)))
                .then(repository.statusHistory(orderId).collectList()
                        .zipWith(repository.stepHistory(orderId).collectList())
                        .map(t -> new History(t.getT1(), t.getT2())));
    }

    /**
     * Cancels a CONFIRMED order (starts the compensation). A lost race with a concurrent change is
     * retried a few times on fresh state before the caller gets a conflict.
     */
    public Mono<Created> cancel(UUID orderId) {
        return Mono.defer(() -> transactions.transactional(coordinator.cancel(orderId)))
                .retryWhen(Retry.max(3).filter(ConcurrentUpdateException.class::isInstance))
                .then(Mono.defer(() -> repository.findOrder(orderId)))
                .map(o -> new Created(o.id(), o.status()));
    }
}
