package com.shop.customer.domain;

import java.util.UUID;

public record Customer(UUID id, String email, String firstName, String lastName, String phone, String address,
                       boolean active, long version) {
}
