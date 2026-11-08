package com.shop.customer.web;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.shop.customer.web.CustomerDtos.CreateCustomerRequest;
import com.shop.customer.web.CustomerDtos.UpdateCustomerRequest;

/** Pure request checks: each returns the list of violations, empty when the request is valid. */
final class RequestValidation {

    static final int MAX_PAGE_SIZE = 100;
    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
    private static final Pattern PHONE = Pattern.compile("^\\+?[0-9 ()-]{6,32}$");

    private RequestValidation() {
    }

    static List<String> check(CreateCustomerRequest r) {
        List<String> errors = new ArrayList<>();
        if (r.email() == null || r.email().isBlank()) {
            errors.add("email is required");
        } else if (r.email().length() > 200 || !EMAIL.matcher(r.email().trim()).matches()) {
            errors.add("email must be a valid e-mail address of at most 200 characters");
        }
        common(errors, r.firstName(), r.lastName(), r.phone(), r.address());
        return errors;
    }

    static List<String> check(UpdateCustomerRequest r) {
        List<String> errors = new ArrayList<>();
        common(errors, r.firstName(), r.lastName(), r.phone(), r.address());
        if (r.active() == null) {
            errors.add("active is required");
        }
        if (r.version() == null || r.version() < 0) {
            errors.add("version is required and must be >= 0");
        }
        return errors;
    }

    private static void common(List<String> errors, String firstName, String lastName, String phone, String address) {
        text(errors, "firstName", firstName, 100);
        text(errors, "lastName", lastName, 100);
        if (phone != null && !phone.isBlank() && !PHONE.matcher(phone.trim()).matches()) {
            errors.add("phone must contain 6 to 32 digits, spaces, parentheses or dashes, optionally starting with +");
        }
        if (address != null && address.length() > 500) {
            errors.add("address must be at most 500 characters");
        }
    }

    private static void text(List<String> errors, String field, String value, int max) {
        if (value == null || value.isBlank()) {
            errors.add(field + " is required");
        } else if (value.length() > max) {
            errors.add(field + " must be at most " + max + " characters");
        }
    }
}
