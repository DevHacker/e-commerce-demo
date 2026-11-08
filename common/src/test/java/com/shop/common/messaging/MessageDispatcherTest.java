package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.shop.common.inbox.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.reactive.TransactionalOperator;

class MessageDispatcherTest {

    private static final String CONSUMER = "product-service.product.commands";

    private final Inbox inbox = mock(Inbox.class);
    private final TransactionalOperator transactions = mock(TransactionalOperator.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final AtomicInteger handled = new AtomicInteger();

    private MessageDispatcher dispatcher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // No real transaction in a unit test: the operator simply returns the Mono it is given.
        when(transactions.transactional(any(Mono.class))).thenAnswer(invocation -> invocation.getArgument(0));
        HandlerRegistry registry = HandlerRegistry.builder()
                .on("ValidateProducts", message -> Mono.fromRunnable(handled::incrementAndGet))
                .on("Boom", message -> Mono.error(new IllegalStateException("database down")))
                .build();
        dispatcher = new MessageDispatcher(CONSUMER, registry, inbox, transactions, Duration.ofSeconds(2));
    }

    private static ConsumerRecord<String, String> record(UUID id, String type) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("product.commands", 0, 1L, "order-1", "{}");
        record.headers().add("id", id.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("type", type.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    /** Inbox that accepts the event and runs the effect, as the real one does for a new event id. */
    @SuppressWarnings("unchecked")
    private void inboxAccepts(UUID id) {
        when(inbox.once(eq(CONSUMER), eq(id), any())).thenAnswer(invocation ->
                ((Supplier<Mono<Void>>) invocation.getArgument(2)).get().thenReturn(true));
    }

    @Test
    void runsTheHandlerThenAcknowledges() {
        UUID id = UUID.randomUUID();
        inboxAccepts(id);

        dispatcher.onMessage(record(id, "ValidateProducts"), acknowledgment);

        assertThat(handled).hasValue(1);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void duplicateEventSkipsTheHandlerButStillAcknowledges() {
        UUID id = UUID.randomUUID();
        when(inbox.once(eq(CONSUMER), eq(id), any())).thenReturn(Mono.just(false));

        dispatcher.onMessage(record(id, "ValidateProducts"), acknowledgment);

        assertThat(handled).hasValue(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void handlerFailureIsPropagatedAndNothingIsAcknowledged() {
        UUID id = UUID.randomUUID();
        inboxAccepts(id);

        assertThatThrownBy(() -> dispatcher.onMessage(record(id, "Boom"), acknowledgment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("database down");

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void unknownTypeIsNotRetryableAndTouchesNothing() {
        UUID id = UUID.randomUUID();

        assertThatThrownBy(() -> dispatcher.onMessage(record(id, "Unknown"), acknowledgment))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("Unknown");

        verify(inbox, never()).once(any(), any(), any());
        verify(acknowledgment, never()).acknowledge();
    }
}
