package com.shop.order.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;

/** Helpers shared by the integration tests: talk to the API, read the outbox, play the other services. */
public abstract class SagaTestBase {

    public static final String REPLIES = "order.saga.replies";

    @Autowired
    protected WebTestClient client;

    @Autowired
    protected DatabaseClient db;

    @Autowired
    protected JsonMapper json;

    @BeforeEach
    protected void cleanDatabase() {
        db.sql("TRUNCATE orders, order_items, saga_state, order_status_history, saga_step_history, "
                        + "outbox_events, processed_events CASCADE")
                .fetch().rowsUpdated().block();
    }

    // ---- the HTTP API ----------------------------------------------------------------------------------

    protected static String orderBody(UUID... productsOfQuantityTwo) {
        StringBuilder items = new StringBuilder();
        for (UUID p : productsOfQuantityTwo) {
            if (items.length() > 0) {
                items.append(',');
            }
            items.append("{\"productId\":\"%s\",\"quantity\":2}".formatted(p));
        }
        return "{\"customerRef\":\"cust-1\",\"items\":[" + items + "],"
                + "\"payment\":{\"method\":\"stripe\",\"details\":{\"paymentMethodId\":\"pm_card_visa\"}}}";
    }

    protected UUID createOrder(UUID... products) {
        return createOrder(UUID.randomUUID().toString(), orderBody(products));
    }

    protected UUID createOrder(String idempotencyKey, String body) {
        String response = client.post().uri("/orders")
                .header("Idempotency-Key", idempotencyKey)
                .header("Content-Type", "application/json")
                .bodyValue(body)
                .exchange()
                .expectStatus().isAccepted()
                .returnResult(String.class).getResponseBody().blockFirst();
        return UUID.fromString(json.readTree(response).get("orderId").asString());
    }

    protected JsonNode order(UUID orderId) {
        String response = client.get().uri("/orders/{id}", orderId)
                .exchange().expectStatus().isOk()
                .returnResult(String.class).getResponseBody().blockFirst();
        return json.readTree(response);
    }

    protected JsonNode history(UUID orderId) {
        String response = client.get().uri("/orders/{id}/history", orderId)
                .exchange().expectStatus().isOk()
                .returnResult(String.class).getResponseBody().blockFirst();
        return json.readTree(response);
    }

    protected HttpStatus cancel(UUID orderId) {
        return HttpStatus.valueOf(client.post().uri("/orders/{id}/cancel", orderId)
                .exchange().returnResult(String.class).getStatus().value());
    }

    protected String statusOf(UUID orderId) {
        return order(orderId).get("status").asString();
    }

    protected void awaitStatus(UUID orderId, String expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(statusOf(orderId)).isEqualTo(expected));
    }

    // ---- the outbox: what the order service sent -------------------------------------------------------

    protected List<Map<String, Object>> outbox(UUID orderId, String type) {
        return db.sql("SELECT topic, aggregateid, type, payload::text AS payload FROM outbox_events "
                        + "WHERE aggregateid = :id AND type = :type ORDER BY occurred_at, id")
                .bind("id", orderId.toString()).bind("type", type)
                .fetch().all().collectList().block();
    }

    protected Map<String, Object> awaitOutbox(UUID orderId, String type) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> !outbox(orderId, type).isEmpty());
        return outbox(orderId, type).get(0);
    }

    protected JsonNode payload(Map<String, Object> outboxRow) {
        return json.readTree((String) outboxRow.get("payload"));
    }

    // ---- the other services: replies as Debezium would deliver them -------------------------------------

    protected static String reply(UUID orderId, String dataJson) {
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"%s\",\"data\":%s}"
                .formatted(orderId, orderId, REPLIES, dataJson);
    }

    protected static String productsValidated(UUID orderId, Map<UUID, String> prices) {
        StringBuilder items = new StringBuilder();
        prices.forEach((p, price) -> {
            if (items.length() > 0) {
                items.append(',');
            }
            items.append("{\"productId\":\"%s\",\"unitPrice\":%s}".formatted(p, price));
        });
        return reply(orderId, "{\"items\":[" + items + "]}");
    }
}
