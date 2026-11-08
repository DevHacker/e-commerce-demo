package com.shop.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.Outcome;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.service.ReservationService;
import com.shop.inventory.service.StockAdminService;
import com.shop.inventory.support.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;

/** The promise of the bucket design: many concurrent orders never oversell and never leave stock behind. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ConcurrentReservationIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        registry.add("shop.r2dbc.pool.max-size", () -> "8");
        registry.add("shop.kafka.bootstrap-servers", () -> "false");
    }

    @Autowired
    ReservationService reservations;

    @Autowired
    StockAdminService admin;

    @Autowired
    TransactionalOperator transactions;

    @Autowired
    DatabaseClient db;

    @BeforeEach
    void clean() {
        db.sql("TRUNCATE product_stock_config, stock_buckets, stock_reservations, stock_movements")
                .fetch().rowsUpdated().block();
    }

    /** Same retry a Kafka consumer gives a message: a deadlock victim is rolled back and run again. */
    private Outcome reserveInOwnTransaction(UUID orderId, UUID product, long quantity) {
        return transactions.transactional(reservations.reserve(orderId, Map.of(product, quantity)))
                .retryWhen(Retry.max(10).filter(DeadlockLoserDataAccessException.class::isInstance))
                .block(Duration.ofSeconds(60));
    }

    private List<Outcome> runConcurrently(UUID product, int orders, long quantityEach, int parallelism) {
        return Flux.range(0, orders)
                .flatMap(i -> transactions.transactional(
                                reservations.reserve(UUID.randomUUID(), Map.of(product, quantityEach)))
                        .retryWhen(Retry.max(10).filter(DeadlockLoserDataAccessException.class::isInstance)),
                        parallelism)
                .collectList()
                .block(Duration.ofSeconds(120));
    }

    private long reservedOf(UUID product) {
        return db.sql("SELECT COALESCE(SUM(reserved), 0) FROM stock_buckets WHERE product_id = :p")
                .bind("p", product).map(r -> r.get(0, Long.class)).one().block();
    }

    @Test
    void neverSellsMoreThanTheStockOfAMultiBucketProduct() {
        UUID product = UUID.randomUUID();
        admin.set(product, 100, 8).block();

        List<Outcome> outcomes = runConcurrently(product, 150, 1, 6);

        assertThat(outcomes.stream().filter(Reserved.class::isInstance)).hasSize(100);
        assertThat(outcomes.stream().filter(Insufficient.class::isInstance)).hasSize(50);
        assertThat(reservedOf(product)).isEqualTo(100);
        assertThat(db.sql("SELECT COUNT(*) FROM stock_buckets WHERE product_id = :p AND reserved > available")
                .bind("p", product).map(r -> r.get(0, Long.class)).one().block()).isZero();
    }

    @Test
    void ordersLargerThanABucketStillNeverOversellAndLeaveOnlyAnUnsellableRemainder() {
        UUID product = UUID.randomUUID();
        admin.set(product, 100, 8).block();

        // 40 orders of 3 units against 100: at most 33 can fit, 1 unit stays free
        List<Outcome> outcomes = runConcurrently(product, 40, 3, 6);

        long reservedOrders = outcomes.stream().filter(Reserved.class::isInstance).count();
        assertThat(reservedOrders).isEqualTo(33);
        assertThat(reservedOf(product)).isEqualTo(99);
        assertThat(db.sql("SELECT COUNT(*) FROM stock_reservations WHERE status = 'RESERVED'")
                .map(r -> r.get(0, Long.class)).one().block()).isPositive();
    }

    @Test
    void releaseAndReserveInterleavedKeepTheBooksBalanced() {
        UUID product = UUID.randomUUID();
        admin.set(product, 20, 4).block();
        List<UUID> orders = IntStream.range(0, 20).mapToObj(i -> UUID.randomUUID()).toList();
        orders.forEach(o -> reserveInOwnTransaction(o, product, 1));
        assertThat(reservedOf(product)).isEqualTo(20);

        Flux.fromIterable(orders)
                .flatMap(o -> transactions.transactional(reservations.release(o))
                        .retryWhen(Retry.max(10).filter(DeadlockLoserDataAccessException.class::isInstance)), 6)
                .blockLast(Duration.ofSeconds(60));

        assertThat(reservedOf(product)).isZero();
        assertThat(db.sql("SELECT COALESCE(SUM(delta), 0) FROM stock_movements WHERE product_id = :p AND type <> 'ADJUST'")
                .bind("p", product).map(r -> r.get(0, Long.class)).one().block()).isZero();
    }
}
