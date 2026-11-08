package com.shop.order.repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.common.messaging.JsonCodec;
import com.shop.order.domain.OrderExceptions.ConcurrentUpdateException;
import com.shop.order.domain.OrderStatus;
import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.Records.Order;
import com.shop.order.domain.Records.OrderItem;
import com.shop.order.domain.Records.Saga;
import com.shop.order.domain.SagaMachine.StepEntry;
import com.shop.order.domain.SagaState;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL of the order aggregate. {@code orders} and {@code saga_state} are updated with optimistic
 * locking ({@code WHERE version = :expected}): a lost race raises {@link ConcurrentUpdateException}
 * instead of overwriting.
 */
@Repository
public class OrderRepository {

    private static final String ORDER_COLUMNS = "id, customer_ref, status, total, payment_method, payment_details::text AS payment_details, idempotency_key, version";

    private final DatabaseClient db;
    private final JsonCodec codec;

    public OrderRepository(DatabaseClient db, JsonCodec codec) {
        this.db = db;
        this.codec = codec;
    }

    public record StatusHistoryRow(OrderStatus from, OrderStatus to, String reason, UUID triggerMessageId,
                                   Instant occurredAt) {
    }

    public record StepHistoryRow(String step, String direction, String outcome, UUID messageId, String detail,
                                 Instant occurredAt) {
    }

    // ---- create -------------------------------------------------------------------------------------------

    public Mono<Void> insertOrder(UUID id, String customerRef, String paymentMethod,
                                  Map<String, String> paymentDetails, String idempotencyKey) {
        return db.sql("""
                        INSERT INTO orders (id, customer_ref, status, payment_method, payment_details, idempotency_key)
                        VALUES (:id, :customerRef, 'PENDING', :method, CAST(:details AS jsonb), :key)""")
                .bind("id", id)
                .bind("customerRef", customerRef)
                .bind("method", paymentMethod)
                .bind("details", codec.write(paymentDetails))
                .bind("key", idempotencyKey)
                .fetch().rowsUpdated().then();
    }

    public Mono<Void> insertItem(UUID orderId, UUID productId, int quantity) {
        return db.sql("INSERT INTO order_items (order_id, product_id, quantity) VALUES (:o, :p, :q)")
                .bind("o", orderId)
                .bind("p", productId)
                .bind("q", quantity)
                .fetch()
                .rowsUpdated()
                .then();
    }

    public Mono<Void> insertSaga(UUID orderId, SagaState state, String step) {
        return db.sql("INSERT INTO saga_state (saga_id, state, current_step) VALUES (:id, :state, :step)")
                .bind("id", orderId)
                .bind("state", state.name())
                .bind("step", step)
                .fetch()
                .rowsUpdated()
                .then();
    }

    // ---- read ---------------------------------------------------------------------------------------------

    public Mono<Order> findOrder(UUID id) {
        return db.sql("SELECT " + ORDER_COLUMNS + " FROM orders WHERE id = :id")
                .bind("id", id).map(this::toOrder).one();
    }

    public Mono<Order> findOrderByIdempotencyKey(String key) {
        return db.sql("SELECT " + ORDER_COLUMNS + " FROM orders WHERE idempotency_key = :key")
                .bind("key", key).map(this::toOrder).one();
    }

    public Flux<OrderItem> findItems(UUID orderId) {
        return db.sql("SELECT product_id, quantity, unit_price FROM order_items WHERE order_id = :o ORDER BY product_id")
                .bind("o", orderId)
                .map(row -> new OrderItem(
                        row.get("product_id", UUID.class), row.get("quantity", Integer.class),
                        row.get("unit_price", BigDecimal.class)))
                .all();
    }

    public Mono<Saga> findSaga(UUID id) {
        return db.sql("SELECT saga_id, state, current_step, version, updated_at FROM saga_state WHERE saga_id = :id")
                .bind("id", id).map(OrderRepository::toSaga).one();
    }

    /** Order, items and saga in one go, empty if the order does not exist. */
    public Mono<Loaded> load(UUID orderId) {
        return findOrder(orderId)
                .flatMap(order -> findSaga(orderId).zipWith(findItems(orderId).collectList())
                .map(t -> new Loaded(t.getT1(), order, t.getT2())));
    }

    /** Sagas that made no progress since {@code cutoff}, oldest first. */
    public Flux<UUID> findStaleSagas(Instant cutoff, int limit) {
        return db.sql("""
                        SELECT saga_id FROM saga_state
                         WHERE state IN ('STARTED', 'PRODUCTS_VALIDATED', 'STOCK_RESERVED', 'COMPENSATING')
                           AND updated_at < :cutoff
                         ORDER BY updated_at LIMIT :limit""")
                .bind("cutoff", OffsetDateTime.ofInstant(cutoff, java.time.ZoneOffset.UTC))
                .bind("limit", limit)
                .map(row -> row.get("saga_id", UUID.class))
                .all();
    }

    public Flux<StatusHistoryRow> statusHistory(UUID orderId) {
        return db.sql("""
                        SELECT from_status, to_status, reason, trigger_message_id, occurred_at
                          FROM order_status_history WHERE order_id = :o ORDER BY id""")
                .bind("o", orderId)
                .map(row -> new StatusHistoryRow(
                        row.get("from_status", String.class) == null ? null
                                : OrderStatus.valueOf(row.get("from_status", String.class)),
                        OrderStatus.valueOf(row.get("to_status", String.class)),
                        row.get("reason", String.class),
                        row.get("trigger_message_id", UUID.class),
                        row.get("occurred_at", OffsetDateTime.class).toInstant()))
                .all();
    }

    public Flux<StepHistoryRow> stepHistory(UUID sagaId) {
        return db.sql("""
                        SELECT step, direction, outcome, message_id, detail::text AS detail, occurred_at
                          FROM saga_step_history WHERE saga_id = :s ORDER BY id""")
                .bind("s", sagaId)
                .map(row -> new StepHistoryRow(
                        row.get("step", String.class), row.get("direction", String.class),
                        row.get("outcome", String.class), row.get("message_id", UUID.class),
                        row.get("detail", String.class), row.get("occurred_at", OffsetDateTime.class).toInstant()))
                .all();
    }

    // ---- write (optimistic) ---------------------------------------------------------------------------------

    public Mono<Void> updateSaga(Saga saga, SagaState newState, String newStep) {
        return db.sql("""
                        UPDATE saga_state SET state = :state, current_step = :step, version = version + 1, updated_at = now()
                         WHERE saga_id = :id AND version = :version""")
                .bind("state", newState.name()).bind("step", newStep)
                .bind("id", saga.id()).bind("version", saga.version())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentUpdateException("Saga", saga.id())));
    }

    public Mono<Void> updateOrderStatus(Order order, OrderStatus newStatus) {
        return db.sql("""
                        UPDATE orders SET status = :status, version = version + 1, updated_at = now()
                         WHERE id = :id AND version = :version""")
                .bind("status", newStatus.name()).bind("id", order.id()).bind("version", order.version())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentUpdateException("Order", order.id())));
    }

    /** Stores the price snapshot: unit prices on the items, total on the order. */
    public Mono<Void> applyPricing(Order order, Map<UUID, BigDecimal> unitPrices, BigDecimal total) {
        return Flux.fromIterable(unitPrices.entrySet())
                .concatMap(e -> db.sql("UPDATE order_items SET unit_price = :price WHERE order_id = :o AND product_id = :p")
                        .bind("price", e.getValue()).bind("o", order.id()).bind("p", e.getKey())
                        .fetch().rowsUpdated())
                .then(db.sql("""
                                UPDATE orders SET total = :total, version = version + 1, updated_at = now()
                                 WHERE id = :id AND version = :version""")
                        .bind("total", total).bind("id", order.id()).bind("version", order.version())
                        .fetch().rowsUpdated())
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentUpdateException("Order", order.id())));
    }

    // ---- histories (append only) ------------------------------------------------------------------------------

    public Mono<Void> insertStatusHistory(UUID orderId, OrderStatus from, OrderStatus to, String reason,
                                          UUID triggerMessageId) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO order_status_history (order_id, from_status, to_status, reason, trigger_message_id)
                        VALUES (:o, :from, :to, :reason, :trigger)""")
                .bind("o", orderId)
                .bind("to", to.name())
                .bind("reason", reason);
        spec = from == null ? spec.bindNull("from", String.class) : spec.bind("from", from.name());
        spec = triggerMessageId == null ? spec.bindNull("trigger", UUID.class) : spec.bind("trigger", triggerMessageId);
        return spec.fetch()
                .rowsUpdated()
                .then();
    }

    public Mono<Void> insertStepHistory(UUID sagaId, StepEntry entry, UUID messageId, String detailJson) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO saga_step_history (saga_id, step, direction, outcome, message_id, detail)
                        VALUES (:s, :step, :direction, :outcome, :message, CAST(:detail AS jsonb))""")
                .bind("s", sagaId).bind("step", entry.step())
                .bind("direction", entry.direction().name()).bind("outcome", entry.outcome());

        spec = messageId == null
                ? spec.bindNull("message", UUID.class)
                : spec.bind("message", messageId);
        spec = detailJson == null
                ? spec.bindNull("detail", String.class)
                : spec.bind("detail", detailJson);

        return spec.fetch().rowsUpdated().then();
    }

    // ---- mapping ------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Order toOrder(Readable row) {
        Map<String, String> details = codec.read(row.get("payment_details", String.class), Map.class);
        return new Order(
                row.get("id", UUID.class),
                row.get("customer_ref", String.class),
                OrderStatus.valueOf(row.get("status", String.class)),
                row.get("total", BigDecimal.class),
                row.get("payment_method", String.class), details,
                row.get("idempotency_key", String.class),
                row.get("version", Long.class)
        );
    }

    private static Saga toSaga(Readable row) {
        return new Saga(
                row.get("saga_id", UUID.class), SagaState.valueOf(row.get("state", String.class)),
                row.get("current_step", String.class), row.get("version", Long.class),
                row.get("updated_at", OffsetDateTime.class).toInstant());
    }
}
