package com.shop.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.shop.common.config.CommonAutoConfiguration;
import com.shop.common.config.KafkaConsumerConfig;
import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.common.support.KafkaSupport;
import com.shop.common.support.KafkaTestClient;
import com.shop.common.support.PostgresSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Real PostgreSQL + real Kafka: dispatch, idempotence, same-transaction outbox, retry, dead letter,
 * and progress after a dead-lettered record.
 */
class MessageDispatcherKafkaIT {

    private static final String CONSUMER = "it-service.it.commands";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CommonAutoConfiguration.class))
            .withPropertyValues(PostgresSupport.r2dbcProperties())
            .withPropertyValues(
                    "shop.retention.enabled=false",
                    "shop.kafka.bootstrap-servers=" + KafkaSupport.bootstrapServers(),
                    "shop.kafka.group-id=it-" + UUID.randomUUID(),
                    "shop.kafka.concurrency=1",
                    "shop.kafka.retry.initial-interval=50ms",
                    "shop.kafka.retry.max-interval=100ms",
                    "shop.kafka.retry.max-attempts=2");

    private static String payload() {
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"order.saga.replies\",\"data\":\"x\"}"
                .formatted(UUID.randomUUID(), UUID.randomUUID());
    }

    @Test
    void dispatchesDeduplicatesRetriesAndDeadLetters() {
        String topic = "it.commands." + UUID.randomUUID();
        String dlt = topic + KafkaConsumerConfig.DLT_SUFFIX;
        AtomicInteger counted = new AtomicInteger();
        AtomicInteger failing = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();

        runner.run(context -> {
            KafkaTestClient kafka = new KafkaTestClient(KafkaSupport.bootstrapServers());
            kafka.createTopics(topic, dlt);

            DatabaseClient db = context.getBean(DatabaseClient.class);
            OutboxWriter outbox = context.getBean(OutboxWriter.class);
            db.sql("TRUNCATE processed_events, outbox_events").fetch().rowsUpdated().block(Duration.ofSeconds(10));

            HandlerRegistry registry = HandlerRegistry.builder()
                    .on("Count", message -> {
                        counted.incrementAndGet();
                        return outbox.write("it.events", "Test", message.key(), "Counted",
                                Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "r", "done")).then();
                    })
                    .on("Fail", message -> {
                        failing.incrementAndGet();
                        return Mono.error(new IllegalStateException("database down"));
                    })
                    .on("Bad", message -> {
                        bad.incrementAndGet();
                        return Mono.error(new NonRetryableException("payload cannot be understood"));
                    })
                    .build();
            MessageDispatcher dispatcher = new MessageDispatcher(CONSUMER, registry,
                    context.getBean(Inbox.class), context.getBean(TransactionalOperator.class), Duration.ofSeconds(10));

            @SuppressWarnings("unchecked")
            ConcurrentKafkaListenerContainerFactory<String, String> factory = context.getBean(
                    KafkaConsumerConfig.CONTAINER_FACTORY, ConcurrentKafkaListenerContainerFactory.class);
            ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topic);
            container.getContainerProperties().setMessageListener(
                    (AcknowledgingMessageListener<String, String>) dispatcher::onMessage);
            container.start();
            try {
                // 1. the same event delivered twice is handled once, and its outbox row is written once
                UUID duplicated = UUID.randomUUID();
                kafka.send(topic, "order-1", duplicated, "Count", payload());
                kafka.send(topic, "order-1", duplicated, "Count", payload());
                kafka.send(topic, "order-2", UUID.randomUUID(), "Count", payload());
                await().atMost(Duration.ofSeconds(30)).untilAtomic(counted, org.hamcrest.Matchers.equalTo(2));
                assertThat(db.sql("SELECT count(*) FROM outbox_events").map(r -> r.get(0, Long.class)).one()
                        .block(Duration.ofSeconds(10))).isEqualTo(2);

                // 2. a transient failure is retried (1 delivery + 2 retries) and then dead-lettered
                UUID failingId = UUID.randomUUID();
                String failingPayload = payload();
                kafka.send(topic, "order-3", failingId, "Fail", failingPayload);
                List<ConsumerRecord<String, String>> deadLetters = kafka.read(dlt, 1, Duration.ofSeconds(30));
                assertThat(deadLetters).hasSize(1);
                assertThat(failing).hasValue(3);
                ConsumerRecord<String, String> deadLetter = deadLetters.get(0);
                assertThat(deadLetter.key()).isEqualTo("order-3");
                assertThat(deadLetter.value()).isEqualTo(failingPayload);
                assertThat(new String(deadLetter.headers().lastHeader("id").value(), StandardCharsets.UTF_8))
                        .isEqualTo(failingId.toString());

                // 3. a non-retryable failure is dead-lettered without any retry
                kafka.send(topic, "order-4", UUID.randomUUID(), "Bad", payload());
                assertThat(kafka.read(dlt, 2, Duration.ofSeconds(30))).hasSize(2);
                assertThat(bad).hasValue(1);

                // 4. the consumer keeps going after dead letters
                kafka.send(topic, "order-5", UUID.randomUUID(), "Count", payload());
                await().atMost(Duration.ofSeconds(30)).untilAtomic(counted, org.hamcrest.Matchers.equalTo(3));
            } finally {
                container.stop();
            }
        });
    }
}
