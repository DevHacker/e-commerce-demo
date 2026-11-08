package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class JsonCodecTest {

    record Ping(String name, int count) {
    }

    private final JsonCodec codec = new JsonCodec(JsonMapper.builder().build());

    @Test
    void roundTripsAnEnvelope() {
        Envelope<Ping> original = Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "order.saga.replies",
                new Ping("a", 2));

        Envelope<Ping> parsed = codec.readEnvelope(codec.write(original), Ping.class);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void withDataKeepsCorrelationMetadata() {
        Envelope<String> command = Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "order.saga.replies", "ask");

        Envelope<Integer> reply = command.withData(42);

        assertThat(reply.sagaId()).isEqualTo(command.sagaId());
        assertThat(reply.correlationId()).isEqualTo(command.correlationId());
        assertThat(reply.replyTo()).isEqualTo("order.saga.replies");
        assertThat(reply.data()).isEqualTo(42);
    }

    @Test
    void invalidJsonIsNotRetryable() {
        assertThatThrownBy(() -> codec.readEnvelope("{not json", Ping.class))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("Envelope<Ping>");
    }

    @Test
    void readsPlainObjects() {
        assertThat(codec.read("{\"name\":\"x\",\"count\":1}", Ping.class)).isEqualTo(new Ping("x", 1));
    }
}
