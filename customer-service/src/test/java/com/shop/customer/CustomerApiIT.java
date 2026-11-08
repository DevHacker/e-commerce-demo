package com.shop.customer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import com.shop.customer.support.PostgresSupport;
import com.shop.customer.web.CustomerDtos.CustomerResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/** Whole service against a real PostgreSQL: Flyway, pool, routes, optimistic locking, error mapping. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "30s")
class CustomerApiIT {

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        PostgresSupport.r2dbcProperties().forEach((k, v) -> registry.add(k, () -> v));
    }

    @Autowired
    WebTestClient client;

    @Autowired
    DatabaseClient db;

    @BeforeEach
    void cleanTable() {
        db.sql("TRUNCATE customers").fetch().rowsUpdated().block();
    }

    record CreateBody(String email, String firstName, String lastName, String phone, String address) {
    }

    record UpdateBody(String firstName, String lastName, String phone, String address, boolean active, long version) {
    }

    private CustomerResponse create(String email) {
        return client.post().uri("/customers")
                .bodyValue(new CreateBody(email, "Fidèle", "Donvide", "+225 07 00 00 00 00", "Abidjan"))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(CustomerResponse.class).returnResult().getResponseBody();
    }

    @Test
    void createsThenReadsACustomerAndLowerCasesTheEmail() {
        CustomerResponse created = create("  Fidele@Example.COM ");

        assertThat(created.email()).isEqualTo("fidele@example.com");
        assertThat(created.active()).isTrue();
        assertThat(created.version()).isZero();

        client.get().uri("/customers/{id}", created.id()).exchange()
                .expectStatus().isOk()
                .expectBody(CustomerResponse.class).isEqualTo(created);
    }

    @Test
    void createReturnsALocationHeader() {
        client.post().uri("/customers")
                .bodyValue(new CreateBody("loc@example.com", "A", "B", null, null))
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().value("Location", loc -> assertThat(loc).startsWith("/customers/"));
    }

    @Test
    void aDuplicateEmailIsAConflictEvenInAnotherCase() {
        create("dup@example.com");

        client.post().uri("/customers")
                .bodyValue(new CreateBody("DUP@example.com", "Other", "Person", null, null))
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("DUPLICATE_EMAIL");
    }

    @Test
    void invalidRequestsListEveryViolation() {
        client.post().uri("/customers")
                .bodyValue(new CreateBody("nope", "", null, "abc", null))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo("INVALID_REQUEST")
                .jsonPath("$.details.length()").isEqualTo(4);
    }

    @Test
    void unknownCustomerIsNotFoundAndMalformedIdIsBadRequest() {
        client.get().uri("/customers/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("CUSTOMER_NOT_FOUND");
        client.get().uri("/customers/not-a-uuid").exchange().expectStatus().isBadRequest();
    }

    @Test
    void updatesWithTheRightVersionAndRefusesAStaleOne() {
        CustomerResponse created = create("upd@example.com");

        CustomerResponse updated = client.put().uri("/customers/{id}", created.id())
                .bodyValue(new UpdateBody("Fidèle T.", "Donvide", null, "Cocody", false, created.version()))
                .exchange()
                .expectStatus().isOk()
                .expectBody(CustomerResponse.class).returnResult().getResponseBody();

        assertThat(updated.firstName()).isEqualTo("Fidèle T.");
        assertThat(updated.phone()).isNull();
        assertThat(updated.active()).isFalse();
        assertThat(updated.version()).isEqualTo(1);
        assertThat(updated.email()).as("the e-mail identifies the customer and cannot be changed").isEqualTo("upd@example.com");

        client.put().uri("/customers/{id}", created.id())
                .bodyValue(new UpdateBody("Late", "Writer", null, null, true, created.version()))
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("VERSION_CONFLICT");
        client.put().uri("/customers/{id}", UUID.randomUUID())
                .bodyValue(new UpdateBody("A", "B", null, null, true, 0))
                .exchange().expectStatus().isNotFound();
    }

    @Test
    void listsInPagesWithHasNext() {
        for (int i = 0; i < 5; i++) {
            create("page" + i + "@example.com");
        }

        client.get().uri("/customers?page=0&size=2").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(2)
                .jsonPath("$.hasNext").isEqualTo(true);
        client.get().uri("/customers?page=2&size=2").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(1)
                .jsonPath("$.hasNext").isEqualTo(false);
        client.get().uri("/customers?size=0").exchange().expectStatus().isBadRequest();
        client.get().uri("/customers?page=abc").exchange().expectStatus().isBadRequest();
    }
}
