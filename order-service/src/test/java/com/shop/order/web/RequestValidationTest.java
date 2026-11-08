package com.shop.order.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.web.OrderDtos.CreateOrderRequest;
import com.shop.order.web.OrderDtos.ItemRequest;
import com.shop.order.web.OrderDtos.PaymentRequest;
import org.junit.jupiter.api.Test;

class RequestValidationTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    private static final PaymentRequest CARD = new PaymentRequest("CREDIT_CARD", Map.of("cardToken", "tok_1"));

    private static CreateOrderRequest request(String customer, ItemRequest... items) {
        return new CreateOrderRequest(customer, List.of(items), CARD);
    }

    @Test
    void acceptsAValidRequest() {
        assertThat(RequestValidation.check("key-1", request("cust", new ItemRequest(A, 2), new ItemRequest(B, 1))))
                .isEmpty();
    }

    @Test
    void requiresTheIdempotencyKey() {
        assertThat(RequestValidation.check(null, request("cust", new ItemRequest(A, 1))))
                .containsExactly("Idempotency-Key header is required");
        assertThat(RequestValidation.check(" ", request("cust", new ItemRequest(A, 1)))).hasSize(1);
        assertThat(RequestValidation.check("k".repeat(101), request("cust", new ItemRequest(A, 1)))).hasSize(1);
    }

    @Test
    void requiresACustomerAndAtLeastOneItem() {
        assertThat(RequestValidation.check("k", new CreateOrderRequest(" ", null, CARD)))
                .containsExactly("customerRef is required", "items must contain at least one item");
        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", List.of(), CARD)))
                .containsExactly("items must contain at least one item");
    }

    @Test
    void checksEveryItem() {
        assertThat(RequestValidation.check("k", request("c",
                new ItemRequest(null, 1), new ItemRequest(A, 0), new ItemRequest(B, 10_001), new ItemRequest(A, null))))
                .containsExactly(
                        "items[0].productId is required",
                        "items[1].quantity must be between 1 and 10000",
                        "items[2].quantity must be between 1 and 10000",
                        "items[3].productId appears more than once",
                        "items[3].quantity must be between 1 and 10000");
    }

    @Test
    void limitsTheNumberOfItems() {
        List<ItemRequest> many = java.util.stream.IntStream.range(0, 101)
                .mapToObj(i -> new ItemRequest(new UUID(0, i), 1)).toList();

        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", many, CARD)))
                .containsExactly("items must contain at most 100 items");
    }

    @Test
    void requiresAKnownPaymentMethodInAnyCase() {
        List<ItemRequest> one = List.of(new ItemRequest(A, 1));

        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", one, null)))
                .containsExactly("payment.method is required");
        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", one, new PaymentRequest(" ", null))))
                .containsExactly("payment.method is required");
        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", one, new PaymentRequest("BITCOIN", null))))
                .containsExactly("payment.method must be one of [CASH_ON_DELIVERY, CREDIT_CARD, PAYPAL, RAZORPAY, STRIPE]");
        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", one, new PaymentRequest("cash_on_delivery", null))))
                .isEmpty();
    }

    @Test
    void paymentDetailsMustBeSmallAndNonBlank() {
        List<ItemRequest> one = List.of(new ItemRequest(A, 1));
        Map<String, String> blank = Map.of("cardToken", " ");

        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", one, new PaymentRequest("STRIPE", blank))))
                .containsExactly("payment.details entries must have a non-blank name and value");
        assertThat(RequestValidation.check("k", new CreateOrderRequest("c", one,
                new PaymentRequest("STRIPE", Map.of("paymentMethodId", "x".repeat(201))))))
                .containsExactly("payment.details.paymentMethodId must be at most 200 characters");
    }
}
