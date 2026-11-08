package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class InboundMessageTest {

    private static ConsumerRecord<String, String> record(String id, String type, String value) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("product.commands", 2, 17L, "order-1", value);
        if (id != null) {
            record.headers().add("id", id.getBytes(StandardCharsets.UTF_8));
        }
        if (type != null) {
            record.headers().add("type", type.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Test
    void readsIdAndTypeFromHeaders() {
        UUID id = UUID.randomUUID();

        InboundMessage message = InboundMessage.from(record(id.toString(), "ValidateProducts", "{}"));

        assertThat(message.eventId()).isEqualTo(id);
        assertThat(message.type()).isEqualTo("ValidateProducts");
        assertThat(message.key()).isEqualTo("order-1");
        assertThat(message.payload()).isEqualTo("{}");
        assertThat(message.topic()).isEqualTo("product.commands");
        assertThat(message.partition()).isEqualTo(2);
        assertThat(message.offset()).isEqualTo(17L);
    }

    @Test
    void missingIdHeaderIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record(null, "ValidateProducts", "{}")))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("'id'");
    }

    @Test
    void missingTypeHeaderIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record(UUID.randomUUID().toString(), null, "{}")))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("'type'");
    }

    @Test
    void idThatIsNotAUuidIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record("42", "ValidateProducts", "{}")))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("not a UUID");
    }

    @Test
    void emptyPayloadIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record(UUID.randomUUID().toString(), "ValidateProducts", null)))
                .isInstanceOf(NonRetryableException.class);
    }
}
