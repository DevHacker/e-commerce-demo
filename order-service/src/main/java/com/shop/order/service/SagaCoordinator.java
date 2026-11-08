package com.shop.order.service;

import java.time.Instant;
import java.util.UUID;

import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.order.domain.OrderExceptions.OrderConflictException;
import com.shop.order.domain.OrderExceptions.OrderNotFoundException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.SagaMachine;
import com.shop.order.domain.SagaMachine.Apply;
import com.shop.order.domain.SagaMachine.Decision;
import com.shop.order.domain.SagaMachine.Ignore;
import com.shop.order.domain.SagaMachine.Refuse;
import com.shop.order.domain.SagaMachine.Transition;
import com.shop.order.domain.SagaMessages.Reply;
import com.shop.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;

/**
 * Imperative shell around the pure {@link SagaMachine}: load the state, ask the machine what to do,
 * apply the answer. Every method assumes it runs inside a transaction opened by its caller (the
 * message dispatcher, the HTTP service, the sweeper), so the state change, both histories and the
 * outbox rows commit together or not at all.
 */
@Service
public class SagaCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SagaCoordinator.class);

    private final OrderRepository repository;
    private final SagaOutbox outbox;
    private final JsonCodec codec;

    public SagaCoordinator(OrderRepository repository, SagaOutbox outbox, JsonCodec codec) {
        this.repository = repository;
        this.outbox = outbox;
        this.codec = codec;
    }

    /** A reply from Product or Inventory. A reply for an unknown order goes to the dead letter topic. */
    public Mono<Void> onReply(UUID orderId, UUID messageId, Reply reply) {
        return repository.load(orderId)
                .switchIfEmpty(Mono.error(() -> new NonRetryableException("Reply for unknown order " + orderId)))
                .flatMap(ctx -> switch (SagaMachine.onReply(ctx, reply)) {
                    case Apply apply -> apply(ctx, apply.transition(), messageId);
                    case Ignore ignore -> {
                        log.info("Order {}: ignored, {}", orderId, ignore.why());
                        yield Mono.<Void>empty();
                    }
                    case Refuse refuse -> Mono.<Void>empty();
                });
    }

    /** Customer cancellation: only a CONFIRMED order. */
    public Mono<Void> cancel(UUID orderId) {
        return repository.load(orderId)
                .switchIfEmpty(Mono.error(() -> new OrderNotFoundException(orderId)))
                .flatMap(ctx -> switch (SagaMachine.cancel(ctx)) {
                    case Apply apply -> apply(ctx, apply.transition(), null);
                    case Refuse refuse -> Mono.<Void>error(new OrderConflictException(refuse.why()));
                    case Ignore ignore -> Mono.<Void>empty();
                });
    }

    /**
     * Timeout of one saga. The staleness is checked again here, on the freshly loaded state, because
     * the saga may have progressed since the sweeper listed it.
     *
     * @return true if a transition was applied
     */
    public Mono<Boolean> timeout(UUID orderId, Instant staleBefore) {
        return repository.load(orderId)
                .filter(ctx -> ctx.saga().updatedAt().isBefore(staleBefore))
                .flatMap(ctx -> switch (SagaMachine.timeout(ctx)) {
                    case Apply apply -> apply(ctx, apply.transition(), null).thenReturn(true);
                    case Ignore ignore -> Mono.just(false);
                    case Refuse refuse -> Mono.just(false);
                })
                .defaultIfEmpty(false);
    }

    private Mono<Void> apply(Loaded ctx, Transition t, UUID messageId) {
        UUID orderId = ctx.order().id();
        OrderStatus current = ctx.order().status();

        Mono<Void> saga = repository.updateSaga(ctx.saga(), t.state(), t.currentStep());

        Mono<Void> orderStatus = t.orderStatus()
                .map(next -> repository.updateOrderStatus(ctx.order(), next)
                        .then(repository.insertStatusHistory(orderId, current, next, t.reason(), messageId)))
                .orElseGet(Mono::empty);

        Mono<Void> pricing = t.pricing()
                .map(p -> repository.applyPricing(ctx.order(), p.unitPrices(), p.total()))
                .orElseGet(Mono::empty);

        Mono<Void> steps = Flux.fromIterable(t.steps())
                .concatMap(entry -> repository.insertStepHistory(orderId, entry, messageId, json(entry.detail())))
                .then();

        Mono<Void> command = t.command().map(c -> outbox.command(orderId, c)).orElseGet(Mono::empty);

        Mono<Void> event = t.event()
                .map(type -> outbox.event(
                        withTotal(ctx, t), type, t.orderStatus().orElse(current), t.reason()))
                .orElseGet(Mono::empty);

        return saga.then(orderStatus).then(pricing).then(steps).then(command).then(event);
    }

    private static com.shop.order.domain.Records.Order withTotal(Loaded ctx, Transition t) {
        var order = ctx.order();
        return t.pricing().isPresent()
                ? new com.shop.order.domain.Records.Order(order.id(), order.customerRef(), order.status(),
                        t.pricing().get().total(), order.paymentMethod(), order.paymentDetails(),
                        order.idempotencyKey(), order.version())
                : order;
    }

    private String json(Object detail) {
        return detail == null ? null : codec.write(detail);
    }
}
