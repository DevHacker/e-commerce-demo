package com.shop.customer.service;

import java.util.UUID;

import com.shop.customer.domain.Customer;
import com.shop.customer.domain.CustomerNotFoundException;
import com.shop.customer.domain.DuplicateEmailException;
import com.shop.customer.domain.VersionConflictException;
import com.shop.customer.repository.CustomerRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Service
public class CustomerService {

    private final CustomerRepository repository;

    public CustomerService(CustomerRepository repository) {
        this.repository = repository;
    }

    public Mono<Customer> create(String email, String firstName, String lastName, String phone, String address) {
        Customer customer = new Customer(UUID.randomUUID(), email, firstName, lastName, phone, address, true, 0);
        return repository.insert(customer)
                .onErrorMap(DuplicateKeyException.class, e -> new DuplicateEmailException(email));
    }

    public Mono<Customer> get(UUID id) {
        return repository.findById(id).switchIfEmpty(Mono.error(() -> new CustomerNotFoundException(id)));
    }

    /** @return one page plus one extra row, so the caller can tell whether a next page exists */
    public Flux<Customer> list(int page, int size) {
        return repository.findPage(size + 1, (long) page * size);
    }

    public Mono<Customer> update(UUID id, String firstName, String lastName, String phone, String address,
                                 boolean active, long expectedVersion) {
        return repository.update(id, firstName, lastName, phone, address, active, expectedVersion)
                .switchIfEmpty(Mono.defer(() -> repository.exists(id)
                        .flatMap(exists -> exists
                                ? Mono.<Customer>error(new VersionConflictException(id))
                                : Mono.<Customer>error(new CustomerNotFoundException(id)))));
    }
}
