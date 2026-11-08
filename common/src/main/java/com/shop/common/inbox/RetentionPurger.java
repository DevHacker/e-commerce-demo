package com.shop.common.inbox;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;

/** Deletes old inbox and outbox rows in small batches so the tables stay lean without long locks. */
public class RetentionPurger {

    private static final Logger log = LoggerFactory.getLogger(RetentionPurger.class);

    private static final String PURGE_INBOX = """
            DELETE FROM processed_events
            WHERE ctid IN (SELECT ctid FROM processed_events WHERE processed_at < :cutoff LIMIT :batch)
            """;
    private static final String PURGE_OUTBOX = """
            DELETE FROM outbox_events
            WHERE ctid IN (SELECT ctid FROM outbox_events WHERE occurred_at < :cutoff LIMIT :batch)
            """;

    private final DatabaseClient db;
    private final Duration inboxRetention;
    private final Duration outboxRetention;
    private final int batchSize;

    public RetentionPurger(DatabaseClient db, Duration inboxRetention, Duration outboxRetention, int batchSize) {
        this.db = db;
        this.inboxRetention = inboxRetention;
        this.outboxRetention = outboxRetention;
        this.batchSize = batchSize;
    }

    /** Purges both tables; returns the total number of deleted rows. */
    public Mono<Long> purge() {
        return purgeInBatches(PURGE_INBOX, inboxRetention)
                .zipWith(purgeInBatches(PURGE_OUTBOX, outboxRetention), Long::sum)
                .doOnNext(total -> {
                    if (total > 0) {
                        log.info("Retention purge deleted {} rows", total);
                    }
                });
    }

    private Mono<Long> purgeInBatches(String sql, Duration retention) {
        OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(retention);
        Mono<Long> oneBatch = Mono.defer(() -> db.sql(sql)
                .bind("cutoff", cutoff)
                .bind("batch", batchSize)
                .fetch()
                .rowsUpdated());
        return oneBatch
                .expand(deleted -> deleted >= batchSize ? oneBatch : Mono.empty())
                .reduce(0L, Long::sum);
    }
}
