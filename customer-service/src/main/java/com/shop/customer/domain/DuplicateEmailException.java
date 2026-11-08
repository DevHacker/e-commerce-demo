package com.shop.customer.domain;

public class DuplicateEmailException extends RuntimeException {

    public DuplicateEmailException(String email) {
        super("A customer with e-mail '" + email + "' already exists");
    }
}
