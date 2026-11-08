package com.shop.common.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** A Kafka record reduced to what handlers need. Built from the headers set by Debezium. */
public record InboundMessage(
        UUID eventId,
        String type,
        String key,
        String payload,
        String topic,
        int partition,
        long offset) {

    public static final String HEADER_ID = "id";
    public static final String HEADER_TYPE = "type";

    public static InboundMessage from(ConsumerRecord<String, String> record) {
        String id = header(record, HEADER_ID)
                .orElseThrow(() -> new NonRetryableException("Missing Kafka header '" + HEADER_ID + "'"));
        String type = header(record, HEADER_TYPE)
                .orElseThrow(() -> new NonRetryableException("Missing Kafka header '" + HEADER_TYPE + "'"));
        if (record.value() == null) {
            throw new NonRetryableException("Empty payload for message type " + type);
        }
        return new InboundMessage(
                parseUuid(id), type, record.key(), record.value(), record.topic(), record.partition(), record.offset());
    }

    public <T> Envelope<T> envelope(JsonCodec codec, Class<T> dataType) {
        return codec.readEnvelope(payload, dataType);
    }

    private static Optional<String> header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return Optional.empty();
        }
        return Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }

    private static UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableException("Header '" + HEADER_ID + "' is not a UUID: " + value, e);
        }
    }
}
