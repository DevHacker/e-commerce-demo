package com.shop.product;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.product.support.PostgresSupport;
import com.shop.product.web.ProductDtos.ProductResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/** Whole service against a real PostgreSQL: Flyway, pool, routes, optimistic locking, error mapping. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class ProductApiIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
        // "false" switches the Kafka beans off (@ConditionalOnProperty), so this test needs no broker
        registry.add("shop.kafka.bootstrap-servers", () -> "false");
    }

    @Autowired
    WebTestClient client;

    @Autowired
    DatabaseClient db;

    @BeforeEach
    void cleanTable() {
        db.sql("TRUNCATE products").fetch().rowsUpdated().block();
    }

    private ProductResponse create(String sku, String price) {
        return client.post().uri("/products")
                .bodyValue(new CreateBody(sku, "Mug " + sku, new BigDecimal(price)))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ProductResponse.class).returnResult().getResponseBody();
    }

    record CreateBody(String sku, String name, BigDecimal price) {
    }

    record UpdateBody(String name, BigDecimal price, boolean active, long version) {
    }

    @Test
    void createsThenReadsAProduct() {
        ProductResponse created = create("SKU-1", "9.99");

        assertThat(created.version()).isZero();
        assertThat(created.active()).isTrue();
        client.get().uri("/products/{id}", created.id())
                .exchange()
                .expectStatus().isOk()
                .expectBody(ProductResponse.class).isEqualTo(created);
    }

    @Test
    void returnsLocationHeaderOnCreate() {
        client.post().uri("/products")
                .bodyValue(new CreateBody("SKU-LOC", "Mug", new BigDecimal("1.00")))
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().value("Location", loc -> assertThat(loc).startsWith("/products/"));
    }

    @Test
    void duplicateSkuIsAConflict() {
        create("SKU-DUP", "1.00");

        client.post().uri("/products")
                .bodyValue(new CreateBody("SKU-DUP", "Other", new BigDecimal("2.00")))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT)
                .expectBody().jsonPath("$.code").isEqualTo("DUPLICATE_SKU");
    }

    @Test
    void invalidBodyListsEveryViolation() {
        client.post().uri("/products")
                .bodyValue(new CreateBody("", null, new BigDecimal("-5")))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo("INVALID_REQUEST")
                .jsonPath("$.details.length()").isEqualTo(3);
    }

    @Test
    void unknownProductIsNotFoundAndMalformedIdIsBadRequest() {
        client.get().uri("/products/{id}", UUID.randomUUID())
                .exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("PRODUCT_NOT_FOUND");

        client.get().uri("/products/not-a-uuid").exchange().expectStatus().isBadRequest();
    }

    @Test
    void updateBumpsTheVersionAndStaleVersionIsAConflict() {
        ProductResponse created = create("SKU-UPD", "5.00");

        ProductResponse updated = client.put().uri("/products/{id}", created.id())
                .bodyValue(new UpdateBody("Renamed", new BigDecimal("6.50"), false, created.version()))
                .exchange()
                .expectStatus().isOk()
                .expectBody(ProductResponse.class).returnResult().getResponseBody();

        assertThat(updated.version()).isEqualTo(1);
        assertThat(updated.name()).isEqualTo("Renamed");
        assertThat(updated.price()).isEqualByComparingTo("6.50");
        assertThat(updated.active()).isFalse();

        // the same (now stale) version a second time
        client.put().uri("/products/{id}", created.id())
                .bodyValue(new UpdateBody("Again", new BigDecimal("7.00"), true, created.version()))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CONFLICT)
                .expectBody().jsonPath("$.code").isEqualTo("VERSION_CONFLICT");
    }

    @Test
    void updatingAnUnknownProductIsNotFound() {
        client.put().uri("/products/{id}", UUID.randomUUID())
                .bodyValue(new UpdateBody("X", BigDecimal.ONE, true, 0))
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void twoConcurrentUpdatesFromTheSameVersionLetExactlyOneWin() {
        ProductResponse created = create("SKU-RACE", "5.00");

        var results = java.util.stream.IntStream.range(0, 8).parallel()
                .mapToObj(i -> client.put().uri("/products/{id}", created.id())
                        .bodyValue(new UpdateBody("Racer " + i, new BigDecimal("1.00"), true, created.version()))
                        .exchange()
                        .returnResult(String.class).getStatus().value())
                .toList();

        assertThat(results.stream().filter(s -> s == 200)).hasSize(1);
        assertThat(results.stream().filter(s -> s == 409)).hasSize(7);
    }

    @Test
    void listsInPagesAndReportsWhetherThereIsANextPage() {
        for (int i = 1; i <= 5; i++) {
            create("SKU-P" + i, "1.00");
        }

        client.get().uri("/products?page=0&size=2").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(2)
                .jsonPath("$.hasNext").isEqualTo(true);

        client.get().uri("/products?page=2&size=2").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(1)
                .jsonPath("$.hasNext").isEqualTo(false);

        client.get().uri("/products?size=1000").exchange().expectStatus().isBadRequest();
    }

    @Test
    void healthEndpointIsUp() {
        client.get().uri("/actuator/health").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
    }
}
