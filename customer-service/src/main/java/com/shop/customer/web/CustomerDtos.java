package com.shop.customer.web;

import java.util.List;
import java.util.UUID;

import com.shop.customer.domain.Customer;

/** Wire format of the HTTP API. */
public final class CustomerDtos {

    private CustomerDtos() {
    }

    public record CreateCustomerRequest(String email, String firstName, String lastName, String phone, String address) {
    }

    public record UpdateCustomerRequest(String firstName, String lastName, String phone, String address,
                                        Boolean active, Long version) {
    }

    public record CustomerResponse(UUID id, String email, String firstName, String lastName, String phone,
                                   String address, boolean active, long version) {

        public static CustomerResponse from(Customer c) {
            return new CustomerResponse(c.id(), c.email(), c.firstName(), c.lastName(), c.phone(), c.address(),
                    c.active(), c.version());
        }
    }

    public record PageResponse<T>(List<T> items, int page, int size, boolean hasNext) {
    }

    public record ApiError(String code, String message, List<String> details) {
    }
}
