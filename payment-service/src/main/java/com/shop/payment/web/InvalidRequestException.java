package com.shop.payment.web;

import java.util.List;

class InvalidRequestException extends RuntimeException {

    private final List<String> details;

    InvalidRequestException(String message, List<String> details) {
        super(message);
        this.details = List.copyOf(details);
    }

    List<String> details() {
        return details;
    }
}
