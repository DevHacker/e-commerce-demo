package com.shop.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.shop.common.config.CommonAutoConfiguration;
import com.shop.common.config.FlywayConfig.FlywayMigration;
import com.shop.common.inbox.Inbox;
import com.shop.common.inbox.RetentionPurger;
import com.shop.common.messaging.Envelope;
import com.shop.common.outbox.OutboxWriter;
import com.shop.common.support.PostgresSupport;
import io.r2dbc.pool.ConnectionPool;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Boots the library against a real PostgreSQL: migration, pool, transactions, outbox, inbox, purge. */
class PostgresInfrastructureIT {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CommonAutoConfiguration.class))
            .withPropertyValues(PostgresSupport.r2dbcProperties())
            .withPropertyValues("shop.retention.enabled=false");

    private static long count(DatabaseClient db, String sql) {
        return db.sql(sql).map(row -> row.get(0, Long.class)).one().block(Duration.ofSeconds(10));
    }

    @Test
    void migratesTheSchemaBeforeThePoolOpens() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(ConnectionPool.class);
            assertThat(context.getBean(FlywayMigration.class).executed()).isTrue();

            long tables = count(context.getBean(DatabaseClient.class), """
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_name IN ('processed_events', 'outbox_events')
                    """);
            assertThat(tables).isEqualTo(2);
        });
    }

    @Test
    void outboxRowIsCommittedWithTheBusinessTransaction() {
        runner.run(context -> {
            OutboxWriter outbox = context.getBean(OutboxWriter.class);
            TransactionalOperator tx = context.getBean(TransactionalOperator.class);
            DatabaseClient db = context.getBean(DatabaseClient.class);
            String orderId = "order-" + UUID.randomUUID();
            UUID sagaId = UUID.randomUUID();

            StepVerifier.create(tx.transactional(outbox.write(
                            "product.commands", "Order", orderId, "ValidateProducts",
                            Envelope.of(sagaId, sagaId, "order.saga.replies", "payload"))))
                    .expectNextCount(1)
                    .verifyComplete();

            String row = db.sql("""
                            SELECT topic || '|' || type || '|' || (payload->>'sagaId') || '|' || (payload->>'replyTo')
                            FROM outbox_events WHERE aggregateid = :id
                            """)
                    .bind("id", orderId)
                    .map(r -> r.get(0, String.class))
                    .one().block(Duration.ofSeconds(10));
            assertThat(row).isEqualTo("product.commands|ValidateProducts|" + sagaId + "|order.saga.replies");
        });
    }

    @Test
    void outboxRowIsRolledBackWhenTheTransactionFails() {
        runner.run(context -> {
            OutboxWriter outbox = context.getBean(OutboxWriter.class);
            TransactionalOperator tx = context.getBean(TransactionalOperator.class);
            DatabaseClient db = context.getBean(DatabaseClient.class);
            String orderId = "order-" + UUID.randomUUID();

            StepVerifier.create(tx.transactional(outbox
                            .write("product.commands", "Order", orderId, "ValidateProducts",
                                    Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "r", "x"))
                            .then(Mono.error(new IllegalStateException("business failure")))))
                    .expectError(IllegalStateException.class)
                    .verify();

            assertThat(count(db, "SELECT count(*) FROM outbox_events WHERE aggregateid = '" + orderId + "'"))
                    .isZero();
        });
    }

    @Test
    void inboxRunsTheEffectOnlyOncePerEvent() {
        runner.run(context -> {
            Inbox inbox = context.getBean(Inbox.class);
            AtomicInteger effects = new AtomicInteger();
            UUID eventId = UUID.randomUUID();

            StepVerifier.create(inbox.once("it.consumer", eventId, () -> Mono.fromRunnable(effects::incrementAndGet)))
                    .expectNext(true).verifyComplete();
            StepVerifier.create(inbox.once("it.consumer", eventId, () -> Mono.fromRunnable(effects::incrementAndGet)))
                    .expectNext(false).verifyComplete();

            assertThat(effects).hasValue(1);
        });
    }

    @Test
    void sameEventIdIsIndependentPerConsumer() {
        runner.run(context -> {
            Inbox inbox = context.getBean(Inbox.class);
            UUID eventId = UUID.randomUUID();

            StepVerifier.create(inbox.once("it.consumer-a", eventId, Mono::empty)).expectNext(true).verifyComplete();
            StepVerifier.create(inbox.once("it.consumer-b", eventId, Mono::empty)).expectNext(true).verifyComplete();
        });
    }

    @Test
    void inboxMarkIsRolledBackWhenTheEffectFails() {
        runner.run(context -> {
            Inbox inbox = context.getBean(Inbox.class);
            TransactionalOperator tx = context.getBean(TransactionalOperator.class);
            UUID eventId = UUID.randomUUID();

            StepVerifier.create(tx.transactional(
                            inbox.once("it.consumer", eventId, () -> Mono.error(new IllegalStateException("boom")))))
                    .expectError(IllegalStateException.class)
                    .verify();

            // The failed attempt left no trace, so the redelivery is processed normally.
            StepVerifier.create(tx.transactional(inbox.once("it.consumer", eventId, Mono::empty)))
                    .expectNext(true)
                    .verifyComplete();
        });
    }

    @Test
    void purgesOldRowsInBatchesAndKeepsRecentOnes() {
        runner.run(context -> {
            DatabaseClient db = context.getBean(DatabaseClient.class);
            db.sql("TRUNCATE processed_events, outbox_events").fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    INSERT INTO processed_events (consumer_name, event_id, processed_at)
                    SELECT 'purge', gen_random_uuid(), now() - interval '10 days' FROM generate_series(1, 25)
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    INSERT INTO processed_events (consumer_name, event_id)
                    SELECT 'purge', gen_random_uuid() FROM generate_series(1, 5)
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload, occurred_at)
                    SELECT gen_random_uuid(), 't', 'A', 'k', 'T', '{}'::jsonb, now() - interval '5 days'
                    FROM generate_series(1, 12)
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));

            RetentionPurger purger = new RetentionPurger(db, Duration.ofDays(7), Duration.ofDays(3), 10);

            StepVerifier.create(purger.purge()).expectNext(37L).verifyComplete();
            assertThat(count(db, "SELECT count(*) FROM processed_events")).isEqualTo(5);
            assertThat(count(db, "SELECT count(*) FROM outbox_events")).isZero();
        });
    }
}
