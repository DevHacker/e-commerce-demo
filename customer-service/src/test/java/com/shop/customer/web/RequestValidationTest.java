package com.shop.customer.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.shop.customer.web.CustomerDtos.CreateCustomerRequest;
import com.shop.customer.web.CustomerDtos.UpdateCustomerRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    private static CreateCustomerRequest create(String email, String first, String last, String phone, String address) {
        return new CreateCustomerRequest(email, first, last, phone, address);
    }

    @Test
    void acceptsAValidCustomerWithOrWithoutOptionalFields() {
        assertThat(RequestValidation.check(create("a@b.co", "Fidèle", "Donvide", "+225 07 00 00 00 00", "Abidjan"))).isEmpty();
        assertThat(RequestValidation.check(create("a@b.co", "Fidèle", "Donvide", null, null))).isEmpty();
        assertThat(RequestValidation.check(create("a@b.co", "Fidèle", "Donvide", " ", " "))).isEmpty();
    }

    @Test
    void requiresEmailAndNames() {
        assertThat(RequestValidation.check(create(null, " ", null, null, null)))
                .containsExactly("email is required", "firstName is required", "lastName is required");
    }

    @Test
    void checksTheShapeOfEmailAndPhone() {
        assertThat(RequestValidation.check(create("not-an-email", "A", "B", null, null)))
                .containsExactly("email must be a valid e-mail address of at most 200 characters");
        assertThat(RequestValidation.check(create("a@b", "A", "B", null, null))).hasSize(1);
        assertThat(RequestValidation.check(create("a@b.co", "A", "B", "abc", null)))
                .containsExactly("phone must contain 6 to 32 digits, spaces, parentheses or dashes, optionally starting with +");
    }

    @Test
    void limitsLengths() {
        assertThat(RequestValidation.check(create("a@b.co", "x".repeat(101), "B", null, "y".repeat(501))))
                .containsExactly("firstName must be at most 100 characters", "address must be at most 500 characters");
    }

    @Test
    void anUpdateNeedsTheActiveFlagAndTheVersion() {
        assertThat(RequestValidation.check(new UpdateCustomerRequest("A", "B", null, null, null, null)))
                .containsExactly("active is required", "version is required and must be >= 0");
        assertThat(RequestValidation.check(new UpdateCustomerRequest("A", "B", null, null, true, -1L))).hasSize(1);
        assertThat(RequestValidation.check(new UpdateCustomerRequest("A", "B", null, null, false, 0L))).isEmpty();
    }
}
