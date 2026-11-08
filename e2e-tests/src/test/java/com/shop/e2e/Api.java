package com.shop.e2e;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** HTTP helpers over the three services, as a client of the public APIs only. */
public final class Api {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private Api() {
    }

    // ---- Product ---------------------------------------------------------------------------------------

    public static UUID createProduct(String price) {
        String sku = "E2E-" + UUID.randomUUID();
        Reply r = send("POST", Stack.PRODUCT + "/products", null,
                "{\"sku\":\"%s\",\"name\":\"Mug %s\",\"price\":%s}".formatted(sku, sku, price));
        require(201, r);
        return UUID.fromString(r.json().get("id").asString());
    }

    public static void deactivate(UUID productId) {
        JsonNode product = send("GET", Stack.PRODUCT + "/products/" + productId, null, null).json();
        Reply r = send("PUT", Stack.PRODUCT + "/products/" + productId, null,
                "{\"name\":\"%s\",\"price\":%s,\"active\":false,\"version\":%d}".formatted(
                        product.get("name").asString(), product.get("price").decimalValue().toPlainString(),
                        product.get("version").asLong()));
        require(200, r);
    }

    // ---- Inventory -------------------------------------------------------------------------------------

    public static void setStock(UUID productId, long quantity, int bucketCount) {
        require(200, send("PUT", Stack.INVENTORY + "/stock/" + productId, null,
                "{\"quantity\":%d,\"bucketCount\":%d}".formatted(quantity, bucketCount)));
    }

    public static JsonNode stock(UUID productId) {
        Reply r = send("GET", Stack.INVENTORY + "/stock/" + productId, null, null);
        require(200, r);
        return r.json();
    }

    // ---- Payment ---------------------------------------------------------------------------------------

    /** The payment of an order, or null when the order never reached the payment step. */
    public static JsonNode payment(UUID orderId) {
        Reply r = send("GET", Stack.PAYMENT + "/payments/" + orderId, null, null);
        if (r.status() == 404) {
            return null;
        }
        require(200, r);
        return r.json();
    }

    // ---- Customer --------------------------------------------------------------------------------------

    public static JsonNode createCustomer(String email) {
        Reply r = send("POST", Stack.CUSTOMER + "/customers", null,
                "{\"email\":\"%s\",\"firstName\":\"Fidèle\",\"lastName\":\"Donvide\"}".formatted(email));
        require(201, r);
        return r.json();
    }

    public static JsonNode customer(String id) {
        Reply r = send("GET", Stack.CUSTOMER + "/customers/" + id, null, null);
        require(200, r);
        return r.json();
    }

    // ---- Order -----------------------------------------------------------------------------------------

    /** Paid by Stripe with a card that is accepted. */
    public static UUID placeOrder(UUID productId, int quantity) {
        return placeOrder(productId, quantity, "STRIPE", "{\"paymentMethodId\":\"pm_card_visa\"}");
    }

    public static UUID placeOrder(UUID productId, int quantity, String paymentMethod, String paymentDetailsJson) {
        String body = ("{\"customerRef\":\"e2e\",\"items\":[{\"productId\":\"%s\",\"quantity\":%d}],"
                + "\"payment\":{\"method\":\"%s\",\"details\":%s}}").formatted(productId, quantity, paymentMethod, paymentDetailsJson);
        Reply r = send("POST", Stack.ORDER + "/orders", UUID.randomUUID().toString(), body);
        require(202, r);
        return UUID.fromString(r.json().get("orderId").asString());
    }

    public static JsonNode order(UUID orderId) {
        Reply r = send("GET", Stack.ORDER + "/orders/" + orderId, null, null);
        require(200, r);
        return r.json();
    }

    public static JsonNode history(UUID orderId) {
        Reply r = send("GET", Stack.ORDER + "/orders/" + orderId + "/history", null, null);
        require(200, r);
        return r.json();
    }

    public static int cancel(UUID orderId) {
        return send("POST", Stack.ORDER + "/orders/" + orderId + "/cancel", null, null).status();
    }

    public static BigDecimal total(UUID orderId) {
        return order(orderId).get("total").decimalValue();
    }

    /** Waits until the order has the given status. The whole chain runs through Debezium, hence the generous limit. */
    public static void awaitStatus(UUID orderId, String status) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> org.assertj.core.api.Assertions.assertThat(order(orderId).get("status").asString()).isEqualTo(status));
    }

    /** Waits until the order is no longer PENDING or CANCELLING, whichever way it ended. */
    public static String awaitFinal(UUID orderId, Duration limit) {
        String[] last = new String[1];
        await().atMost(limit).pollInterval(Duration.ofMillis(250)).until(() -> {
            last[0] = order(orderId).get("status").asString();
            return !last[0].equals("PENDING") && !last[0].equals("CANCELLING");
        });
        return last[0];
    }

    /** The reason of the most recent status change. */
    public static String lastReason(UUID orderId) {
        JsonNode status = history(orderId).get("status");
        return status.get(status.size() - 1).path("reason").asString();
    }

    // ---- plumbing --------------------------------------------------------------------------------------

    private record Reply(int status, String body) {
        JsonNode json() {
            return JSON.readTree(body);
        }
    }

    private static Reply send(String method, String url, String idempotencyKey, String body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json");
        if (idempotencyKey != null) {
            b.header("Idempotency-Key", idempotencyKey);
        }
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        try {
            HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
            return new Reply(r.statusCode(), r.body());
        } catch (IOException e) {
            throw new IllegalStateException(method + " " + url + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void require(int expected, Reply r) {
        if (r.status() != expected) {
            throw new AssertionError("expected HTTP " + expected + " but got " + r.status() + ": " + r.body());
        }
    }
}
