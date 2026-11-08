package com.shop.common.inbox;

import java.util.UUID;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Idempotent-consumer guard backed by the {@code processed_events} table.
 * Run it inside the same transaction as the business effect: either both commit or neither does.
 */
public class Inbox {

    private static final String INSERT = """
            INSERT INTO processed_events (consumer_name, event_id)
            VALUES (:consumerName, :eventId)
            ON CONFLICT DO NOTHING
            """;

    private final DatabaseClient db;

    public Inbox(DatabaseClient db) {
        this.db = db;
    }

    /**
     * Records the event, then runs {@code effect}.
     *
     * @return {@code true} if the effect ran, {@code false} if the event had already been processed
     */
    public Mono<Boolean> once(String consumerName, UUID eventId, Supplier<Mono<Void>> effect) {
        return db.sql(INSERT)
                .bind("consumerName", consumerName)
                .bind("eventId", eventId)
                .fetch()
                .rowsUpdated()
                .flatMap(inserted -> inserted == 0
                        ? Mono.just(false)
                        : Mono.defer(effect).thenReturn(true));
    }
}
