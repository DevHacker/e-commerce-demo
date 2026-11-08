package com.shop.inventory.web;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.repository.StockRepository.BucketRow;
import com.shop.inventory.repository.StockRepository.StockOverview;

/** Wire format of the admin HTTP API. */
public final class StockDtos {

    private StockDtos() {
    }

    public record SetStockRequest(Long quantity, Integer bucketCount) {
    }

    public record BucketResponse(int bucketNo, long onHand, long reserved, long free) {
    }

    /** Displayed availability is the sum over buckets (eventually consistent under load). */
    public record StockResponse(UUID productId, int bucketCount, long onHand, long reserved, long free,
                                List<BucketResponse> buckets) {

        public static StockResponse from(UUID productId, StockOverview overview) {
            List<BucketResponse> buckets = overview.buckets().stream().map(StockDtos::bucket).toList();
            return new StockResponse(
                    productId,
                    overview.bucketCount(),
                    buckets.stream().mapToLong(BucketResponse::onHand).sum(),
                    buckets.stream().mapToLong(BucketResponse::reserved).sum(),
                    buckets.stream().mapToLong(BucketResponse::free).sum(),
                    buckets);
        }
    }

    private static BucketResponse bucket(BucketRow row) {
        return new BucketResponse(row.bucketNo(), row.available(), row.reserved(), row.available() - row.reserved());
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
