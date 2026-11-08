package com.shop.inventory.repository;

import java.util.UUID;

import com.shop.inventory.domain.Allocation.Take;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL of the stock. Reservation statements are single atomic UPDATEs: the row lock lives for one
 * statement's execution plus the surrounding transaction, no SELECT ... FOR UPDATE, no read-modify-write
 * in the application.
 */
@Repository
public class StockRepository {

    private final DatabaseClient db;

    public StockRepository(DatabaseClient db) {
        this.db = db;
    }

    public record BucketRow(int bucketNo, long available, long reserved) {
    }

    public record StockOverview(int bucketCount, java.util.List<BucketRow> buckets) {
    }

    // ---- reservation side ------------------------------------------------------------------------------

    public Mono<Integer> bucketCount(UUID productId) {
        return db.sql("SELECT bucket_count FROM product_stock_config WHERE product_id = :p")
                .bind("p", productId)
                .map(row -> row.get("bucket_count", Integer.class))
                .one();
    }

    /**
     * Takes up to {@code need} units of free stock from one bucket, atomically.
     * Both SET expressions read the same locked row, so {@code last_taken} is exactly what was added to
     * {@code reserved}. A bucket with no free stock matches no row: no write, no lock.
     *
     * @return the quantity taken, 0 if the bucket has no free stock
     */
    public Mono<Long> take(UUID productId, int bucketNo, long need) {
        return db.sql("""
                        UPDATE stock_buckets
                           SET reserved   = reserved + LEAST(available - reserved, :need),
                               last_taken = LEAST(available - reserved, :need)
                         WHERE product_id = :p AND bucket_no = :b AND available > reserved
                        RETURNING last_taken""")
                .bind("need", need)
                .bind("p", productId)
                .bind("b", bucketNo)
                .map(row -> row.get("last_taken", Long.class))
                .one()
                .defaultIfEmpty(0L);
    }

    /** Gives reserved units back to the free stock of a bucket (undo of a take, or a release). */
    public Mono<Void> giveBack(Take take) {
        return db.sql("""
                        UPDATE stock_buckets SET reserved = reserved - :qty
                         WHERE product_id = :p AND bucket_no = :b""")
                .bind("qty", take.quantity())
                .bind("p", take.productId())
                .bind("b", take.bucketNo())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1
                        ? Mono.<Void>empty()
                        : Mono.error(new IllegalStateException("Bucket not found: " + take)));
    }

    public Mono<Void> insertReservation(UUID orderId, Take take) {
        return db.sql("""
                        INSERT INTO stock_reservations (order_id, product_id, bucket_no, qty, status)
                        VALUES (:o, :p, :b, :qty, 'RESERVED')""")
                .bind("o", orderId)
                .bind("p", take.productId())
                .bind("b", take.bucketNo())
                .bind("qty", take.quantity())
                .fetch().rowsUpdated().then();
    }

    /** Marks the order's active reservations as released and returns them, in one statement. */
    public Flux<Take> releaseReservations(UUID orderId) {
        return db.sql("""
                        UPDATE stock_reservations SET status = 'RELEASED', updated_at = now()
                         WHERE order_id = :o AND status = 'RESERVED'
                        RETURNING product_id, bucket_no, qty""")
                .bind("o", orderId)
                .map(row -> new Take(
                        row.get("product_id", UUID.class),
                        row.get("bucket_no", Integer.class),
                        row.get("qty", Long.class)))
                .all();
    }

    /** @param orderId the order behind the movement, {@code null} for an admin ADJUST */
    public Mono<Void> insertMovement(UUID productId, UUID orderId, long delta, String type) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO stock_movements (product_id, order_id, delta, type)
                        VALUES (:p, :o, :delta, :type)""")
                .bind("p", productId)
                .bind("delta", delta)
                .bind("type", type);
        spec = orderId == null ? spec.bindNull("o", UUID.class) : spec.bind("o", orderId);
        return spec.fetch().rowsUpdated().then();
    }

    // ---- admin side ------------------------------------------------------------------------------------

    public Mono<StockOverview> overview(UUID productId) {
        return db.sql("""
                        SELECT c.bucket_count, b.bucket_no, b.available, b.reserved
                          FROM product_stock_config c
                          JOIN stock_buckets b ON b.product_id = c.product_id
                         WHERE c.product_id = :p
                         ORDER BY b.bucket_no""")
                .bind("p", productId)
                .map(row -> new Object[] {
                        row.get("bucket_count", Integer.class),
                        new BucketRow(
                                row.get("bucket_no", Integer.class),
                                row.get("available", Long.class),
                                row.get("reserved", Long.class))})
                .all()
                .collectList()
                .filter(rows -> !rows.isEmpty())
                .map(rows -> new StockOverview(
                        (Integer) rows.get(0)[0],
                        rows.stream().map(r -> (BucketRow) r[1]).toList()));
    }

    public Mono<Long> totalReserved(UUID productId) {
        return sum("SELECT COALESCE(SUM(reserved), 0) AS total FROM stock_buckets WHERE product_id = :p", productId);
    }

    public Mono<Long> totalFree(UUID productId) {
        return sum("SELECT COALESCE(SUM(available - reserved), 0) AS total FROM stock_buckets WHERE product_id = :p",
                productId);
    }

    private Mono<Long> sum(String sql, UUID productId) {
        return db.sql(sql).bind("p", productId).map(row -> row.get("total", Long.class)).one();
    }

    public Mono<Void> upsertConfig(UUID productId, int bucketCount) {
        return db.sql("""
                        INSERT INTO product_stock_config (product_id, bucket_count) VALUES (:p, :n)
                        ON CONFLICT (product_id) DO UPDATE SET bucket_count = :n, updated_at = now()""")
                .bind("p", productId)
                .bind("n", bucketCount)
                .fetch().rowsUpdated().then();
    }

    /** Sets the FREE stock of a bucket to {@code freeShare}, keeping whatever is reserved in it. */
    public Mono<Void> upsertBucket(UUID productId, int bucketNo, long freeShare) {
        return db.sql("""
                        INSERT INTO stock_buckets (product_id, bucket_no, available, reserved)
                        VALUES (:p, :b, :share, 0)
                        ON CONFLICT (product_id, bucket_no)
                        DO UPDATE SET available = stock_buckets.reserved + :share""")
                .bind("p", productId)
                .bind("b", bucketNo)
                .bind("share", freeShare)
                .fetch().rowsUpdated().then();
    }

    /** Removes the buckets numbered {@code bucketCount} and above that hold no reservation. */
    public Mono<Void> deleteUnusedBucketsFrom(UUID productId, int bucketCount) {
        return db.sql("DELETE FROM stock_buckets WHERE product_id = :p AND bucket_no >= :n AND reserved = 0")
                .bind("p", productId)
                .bind("n", bucketCount)
                .fetch().rowsUpdated().then();
    }

    public Mono<Long> bucketsFrom(UUID productId, int bucketCount) {
        return db.sql("SELECT count(*) AS total FROM stock_buckets WHERE product_id = :p AND bucket_no >= :n")
                .bind("p", productId)
                .bind("n", bucketCount)
                .map(row -> row.get("total", Long.class))
                .one();
    }
}
