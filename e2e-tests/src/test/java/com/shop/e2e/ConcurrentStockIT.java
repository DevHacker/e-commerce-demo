package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;

/** Many orders racing for one product spread over several stock buckets: never oversold, all sagas end. */
class ConcurrentStockIT {

    @BeforeAll
    static void stack() {
        Stack.start();
    }

    @Test
    void ordersRacingForAMultiBucketProductNeverOversell() throws Exception {
        UUID product = Api.createProduct("1.00");
        Api.setStock(product, 100, 4);
        int orders = 30;
        int quantity = 5; // 150 requested for 100 in stock

        List<UUID> ids = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(15);
        try {
            List<Future<UUID>> futures = new ArrayList<>();
            for (int i = 0; i < orders; i++) {
                futures.add(pool.submit(() -> Api.placeOrder(product, quantity)));
            }
            for (Future<UUID> f : futures) {
                ids.add(f.get());
            }
        } finally {
            pool.shutdown();
        }

        int confirmed = 0;
        for (UUID id : ids) {
            String end = Api.awaitFinal(id, Duration.ofSeconds(120));
            assertThat(end).isIn("CONFIRMED", "REJECTED");
            if (end.equals("CONFIRMED")) {
                confirmed++;
            }
        }

        JsonNode stock = Api.stock(product);
        assertThat(confirmed).isBetween(1, 20);
        assertThat(stock.get("reserved").asLong()).isEqualTo(confirmed * (long) quantity);
        assertThat(stock.get("free").asLong()).isGreaterThanOrEqualTo(0);
        for (JsonNode bucket : stock.get("buckets")) {
            assertThat(bucket.get("reserved").asLong()).isBetween(0L, bucket.get("onHand").asLong());
        }
    }
}
