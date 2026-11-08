package com.shop.inventory.service;

import java.util.UUID;

import com.shop.inventory.domain.StockConflictException;
import com.shop.inventory.domain.StockNotFoundException;
import com.shop.inventory.domain.StockShares;
import com.shop.inventory.repository.StockRepository;
import com.shop.inventory.repository.StockRepository.StockOverview;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Admin operations: set the quantity of a product and the number of buckets it is split into. */
@Service
public class StockAdminService {

    private final StockRepository stock;
    private final TransactionalOperator transactions;

    public StockAdminService(StockRepository stock, TransactionalOperator transactions) {
        this.stock = stock;
        this.transactions = transactions;
    }

    public Mono<StockOverview> get(UUID productId) {
        return stock.overview(productId).switchIfEmpty(Mono.error(() -> new StockNotFoundException(productId)));
    }

    /**
     * Sets the on-hand quantity to {@code quantity} spread over {@code bucketCount} buckets. Active
     * reservations are kept: the free stock becomes {@code quantity - reserved}, split evenly. Fails with a
     * conflict (and changes nothing) if {@code quantity} is below what is reserved, or if shrinking the
     * bucket count would drop a bucket that still holds reservations.
     */
    public Mono<StockOverview> set(UUID productId, long quantity, int bucketCount) {
        return transactions.transactional(
                        stock.totalReserved(productId).zipWith(stock.totalFree(productId))
                                .flatMap(tuple -> apply(productId, quantity, bucketCount, tuple.getT1(), tuple.getT2())))
                .then(Mono.defer(() -> get(productId)));
    }

    private Mono<Void> apply(UUID productId, long quantity, int bucketCount, long reserved, long oldFree) {
        if (quantity < reserved) {
            return Mono.error(new StockConflictException(
                    "Quantity " + quantity + " is below the " + reserved + " units currently reserved"));
        }
        long newFree = quantity - reserved;
        long[] shares = StockShares.split(newFree, bucketCount);
        return stock.upsertConfig(productId, bucketCount)
                .thenMany(Flux.range(0, bucketCount).concatMap(b -> stock.upsertBucket(productId, b, shares[b])))
                .then(stock.deleteUnusedBucketsFrom(productId, bucketCount))
                .then(stock.bucketsFrom(productId, bucketCount))
                .flatMap(left -> left > 0
                        ? Mono.<Void>error(new StockConflictException(
                                "Cannot reduce to " + bucketCount + " buckets: a removed bucket still holds reservations"))
                        : stock.insertMovement(productId, null, newFree - oldFree, "ADJUST"));
    }
}
