package com.shop.inventory.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.inventory.domain.Allocation;
import com.shop.inventory.domain.Allocation.Insufficient;
import com.shop.inventory.domain.Allocation.Outcome;
import com.shop.inventory.domain.Allocation.ProductAllocation;
import com.shop.inventory.domain.Allocation.Reserved;
import com.shop.inventory.domain.Allocation.Take;
import com.shop.inventory.domain.BucketScan;
import com.shop.inventory.repository.StockRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Service;

/**
 * Reservation and release of stock for one order. Both methods assume they run inside the caller's
 * transaction (the message dispatcher's), so everything they write commits or rolls back together with
 * the inbox row and the outbox reply.
 */
@Service
public class ReservationService {

    private final StockRepository stock;

    public ReservationService(StockRepository stock) {
        this.stock = stock;
    }

    /**
     * Covers every need from the buckets, product by product in sorted order. If any product cannot be
     * fully covered, every unit taken so far is given back and nothing else is written: no phantom
     * reservation survives, and the caller answers with the shortages.
     *
     * @param needs quantity per product, already sorted (see {@code StockNeeds.merge})
     */
    public Mono<Outcome> reserve(UUID orderId, Map<UUID, Long> needs) {
        return Flux.fromIterable(needs.entrySet())
                .concatMap(need -> allocate(orderId, need.getKey(), need.getValue()))
                .collectList()
                .flatMap(allocations -> switch (Allocation.decide(allocations)) {
                    case Reserved reserved -> record(orderId, reserved.takes()).thenReturn((Outcome) reserved);
                    case Insufficient insufficient -> undo(allocations).thenReturn((Outcome) insufficient);
                });
    }

    /**
     * Returns exactly what the order reserved, bucket by bucket. An order with no active reservation
     * (never reserved, or already released) releases nothing and that is a success.
     *
     * @return the released takes (possibly empty)
     */
    public Mono<List<Take>> release(UUID orderId) {
        return stock.releaseReservations(orderId)
                .collectList()
                .flatMap(takes -> Flux.fromIterable(takes)
                        .concatMap(take -> stock.giveBack(take)
                                .then(stock.insertMovement(take.productId(), orderId, take.quantity(), "RELEASE")))
                        .then(Mono.just(takes)));
    }

    // One product: visit the buckets in scan order until the need is covered or the buckets run out.
    private Mono<ProductAllocation> allocate(UUID orderId, UUID productId, long need) {
        return stock.bucketCount(productId)
                .defaultIfEmpty(0)
                .flatMap(bucketCount -> Mono.just(new Scan(need, List.of(), BucketScan.order(orderId, bucketCount)))
                        .expand(scan -> scan.finished() ? Mono.empty() : step(productId, scan))
                        .last())
                .map(scan -> new ProductAllocation(productId, need, scan.takes(), scan.remaining()));
    }

    private Mono<Scan> step(UUID productId, Scan scan) {
        int bucketNo = scan.pending().get(0);
        List<Integer> rest = scan.pending().subList(1, scan.pending().size());
        return stock.take(productId, bucketNo, scan.remaining()).map(taken -> {
            List<Take> takes = new ArrayList<>(scan.takes());
            if (taken > 0) {
                takes.add(new Take(productId, bucketNo, taken));
            }
            return new Scan(scan.remaining() - taken, List.copyOf(takes), rest);
        });
    }

    /** Scan state: what is still needed, what was taken, which buckets are left to visit. */
    private record Scan(long remaining, List<Take> takes, List<Integer> pending) {

        boolean finished() {
            return remaining == 0 || pending.isEmpty();
        }
    }

    private Mono<Void> record(UUID orderId, List<Take> takes) {
        return Flux.fromIterable(takes)
                .concatMap(take -> stock.insertReservation(orderId, take)
                        .then(stock.insertMovement(take.productId(), orderId, -take.quantity(), "RESERVE")))
                .then();
    }

    private Mono<Void> undo(List<ProductAllocation> allocations) {
        return Flux.fromIterable(allocations)
                .flatMapIterable(ProductAllocation::takes)
                .concatMap(stock::giveBack)
                .then();
    }
}
