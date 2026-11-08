package com.shop.customer.repository;

import java.util.UUID;

import com.shop.customer.domain.Customer;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/** All SQL of the customer registry. Every method is lazy: nothing runs until subscribed. */
@Repository
public class CustomerRepository {

    private static final String COLUMNS = "id, email, first_name, last_name, phone, address, active, version";

    private final DatabaseClient db;

    public CustomerRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Customer> insert(Customer c) {
        return nullable(nullable(db.sql("""
                        INSERT INTO customers (id, email, first_name, last_name, phone, address, active)
                        VALUES (:id, :email, :first, :last, :phone, :address, :active)
                        RETURNING""" + " " + COLUMNS)
                .bind("id", c.id())
                .bind("email", c.email())
                .bind("first", c.firstName())
                .bind("last", c.lastName())
                .bind("active", c.active()), "phone", c.phone()), "address", c.address())
                .map(CustomerRepository::toCustomer)
                .one();
    }

    public Mono<Customer> findById(UUID id) {
        return db.sql("SELECT " + COLUMNS + " FROM customers WHERE id = :id")
                .bind("id", id)
                .map(CustomerRepository::toCustomer)
                .one();
    }

    public Flux<Customer> findPage(int limit, long offset) {
        return db.sql("SELECT " + COLUMNS + " FROM customers ORDER BY created_at, id LIMIT :limit OFFSET :offset")
                .bind("limit", limit).bind("offset", offset).map(CustomerRepository::toCustomer).all();
    }

    /** Optimistic update: empty when the customer does not exist or the version is stale. */
    public Mono<Customer> update(UUID id, String firstName, String lastName, String phone, String address,
                                 boolean active, long expectedVersion) {
        return nullable(nullable(db.sql("""
                        UPDATE customers
                           SET first_name = :first, last_name = :last, phone = :phone, address = :address,
                               active = :active, version = version + 1, updated_at = now()
                         WHERE id = :id AND version = :version
                        RETURNING""" + " " + COLUMNS)
                .bind("id", id).bind("first", firstName).bind("last", lastName).bind("active", active)
                .bind("version", expectedVersion), "phone", phone), "address", address)
                .map(CustomerRepository::toCustomer).one();
    }

    public Mono<Boolean> exists(UUID id) {
        return db.sql("SELECT 1 FROM customers WHERE id = :id").bind("id", id)
                .map(row -> Boolean.TRUE).one().defaultIfEmpty(Boolean.FALSE);
    }

    private static DatabaseClient.GenericExecuteSpec nullable(DatabaseClient.GenericExecuteSpec spec, String name, String value) {
        return value == null ? spec.bindNull(name, String.class) : spec.bind(name, value);
    }

    private static Customer toCustomer(Readable row) {
        return new Customer(
                row.get("id", UUID.class), row.get("email", String.class),
                row.get("first_name", String.class),
                row.get("last_name", String.class),
                row.get("phone", String.class),
                row.get("address", String.class),
                Boolean.TRUE.equals(row.get("active", Boolean.class)),
                row.get("version", Long.class));
    }
}
