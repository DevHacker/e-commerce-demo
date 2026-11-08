package com.shop.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;

/** The customer registry runs next to the others as its own process with its own database. */
class CustomerIT {

    @BeforeAll
    static void stack() {
        Stack.start();
    }

    @Test
    void aCustomerIsRegisteredAndReadBack() {
        JsonNode created = Api.createCustomer("e2e-" + UUID.randomUUID() + "@example.com");

        JsonNode read = Api.customer(created.get("id").asString());

        assertThat(read.get("email").asString()).isEqualTo(created.get("email").asString());
        assertThat(read.get("active").asBoolean()).isTrue();
    }
}
