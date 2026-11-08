package com.shop.common.outbox;

import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.JsonCodec;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Writes a message to the {@code outbox_events} table. Debezium turns the row into a Kafka record.
 * Call it inside the transaction that changes the business state.
 */
public class OutboxWriter {

    private static final String INSERT = """
            INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
            VALUES (:id, :topic, :aggregateType, :aggregateId, :type, CAST(:payload AS jsonb))
            """;

    private final DatabaseClient db;
    private final JsonCodec codec;

    public OutboxWriter(DatabaseClient db, JsonCodec codec) {
        this.db = db;
        this.codec = codec;
    }

    /**
     * @param topic         destination Kafka topic (Debezium routes on this column)
     * @param aggregateType kind of aggregate, e.g. {@code Order}
     * @param aggregateId   Kafka record key; use the order id to keep per-order ordering
     * @param type          message type, becomes the Kafka header {@code type}
     * @return the id of the outbox row, which is also the Kafka header {@code id}
     */
    public <T> Mono<UUID> write(String topic, String aggregateType, String aggregateId, String type,
                                Envelope<T> envelope) {
        UUID id = UUID.randomUUID();
        return Mono.fromSupplier(() -> codec.write(envelope))
                .flatMap(json -> db.sql(INSERT)
                        .bind("id", id)
                        .bind("topic", topic)
                        .bind("aggregateType", aggregateType)
                        .bind("aggregateId", aggregateId)
                        .bind("type", type)
                        .bind("payload", json)
                        .fetch()
                        .rowsUpdated())
                .thenReturn(id);
    }
}
