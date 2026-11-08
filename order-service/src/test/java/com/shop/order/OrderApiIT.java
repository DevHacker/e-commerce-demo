package com.shop.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import com.shop.order.support.PostgresSupport;
import com.shop.order.support.SagaTestBase;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The HTTP side against a real PostgreSQL: one transaction creates order, saga, histories and outbox rows. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class OrderApiIT extends SagaTestBase {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        // "false" switches the Kafka beans off (@ConditionalOnProperty), so this test needs no broker
        registry.add("shop.kafka.bootstrap-servers", () -> "false");
    }

    private final UUID a = UUID.randomUUID();
    private final UUID b = UUID.randomUUID();

    @Test
    void creatingAnOrderStartsTheSagaInOneTransaction() {
        UUID orderId = createOrder(a, b);

        var order = order(orderId);
        assertThat(order.get("status").asString()).isEqualTo("PENDING");
        assertThat(order.get("total").isNull()).isTrue();
        assertThat(order.get("items")).hasSize(2);

        // the command for the Product service and the domain event, both in the outbox
        var command = awaitOutbox(orderId, "ValidateProducts");
        assertThat(command.get("topic")).isEqualTo("product.commands");
        var data = payload(command);
        assertThat(data.get("sagaId").asString()).isEqualTo(orderId.toString());
        assertThat(data.get("replyTo").asString()).isEqualTo("order.saga.replies");
        assertThat(data.get("data").get("items")).hasSize(2);
        assertThat(data.get("data").get("items").get(0).get("quantity").asInt()).isEqualTo(2);

        var event = awaitOutbox(orderId, "OrderCreated");
        assertThat(event.get("topic")).isEqualTo("order.events");

        // both histories
        var history = history(orderId);
        assertThat(history.get("status")).hasSize(1);
        assertThat(history.get("status").get(0).get("from").isNull()).isTrue();
        assertThat(history.get("status").get(0).get("to").asString()).isEqualTo("PENDING");
        assertThat(history.get("steps")).hasSize(1);
        assertThat(history.get("steps").get(0).get("step").asString()).isEqualTo("ValidateProducts");
        assertThat(history.get("steps").get(0).get("outcome").asString()).isEqualTo("REQUESTED");
    }

    @Test
    void theSameIdempotencyKeyReturnsTheSameOrder() {
        String body = orderBody(a);

        UUID first = createOrder("key-1", body);
        UUID second = createOrder("key-1", body);

        assertThat(second).isEqualTo(first);
        assertThat(db.sql("SELECT COUNT(*) FROM orders").map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
        assertThat(outbox(first, "ValidateProducts")).hasSize(1);
    }

    @Test
    void identicalRequestsRacingCreateOneOrder() {
        String body = orderBody(a);

        List<UUID> ids = IntStream.range(0, 8).parallel()
                .mapToObj(i -> createOrder("race-key", body)).toList();

        assertThat(ids).containsOnly(ids.get(0));
        assertThat(db.sql("SELECT COUNT(*) FROM orders").map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
        assertThat(db.sql("SELECT COUNT(*) FROM outbox_events WHERE type = 'ValidateProducts'")
                .map(r -> r.get(0, Long.class)).one().block()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidRequestsWithEveryViolation() {
        client.post().uri("/orders")
                .header("Content-Type", "application/json")
                .bodyValue("{\"customerRef\":\"\",\"items\":[]}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo("INVALID_REQUEST")
                .jsonPath("$.details.length()").isEqualTo(4);
        assertThat(db.sql("SELECT COUNT(*) FROM orders").map(r -> r.get(0, Long.class)).one().block()).isZero();
    }

    @Test
    void unknownOrderIsNotFoundAndMalformedIdIsBadRequest() {
        client.get().uri("/orders/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("ORDER_NOT_FOUND");
        client.get().uri("/orders/{id}/history", UUID.randomUUID()).exchange().expectStatus().isNotFound();
        client.get().uri("/orders/not-a-uuid").exchange().expectStatus().isBadRequest();
    }

    @Test
    void anOrderThatIsNotConfirmedCannotBeCancelled() {
        UUID orderId = createOrder(a);

        assertThat(cancel(orderId)).isEqualTo(HttpStatus.CONFLICT);
        assertThat(cancel(UUID.randomUUID())).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(statusOf(orderId)).isEqualTo("PENDING");
    }
}
