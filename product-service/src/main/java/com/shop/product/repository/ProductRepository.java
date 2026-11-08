package com.shop.product.repository;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.UUID;

import com.shop.product.domain.Product;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/** All SQL of the catalogue. Every method is lazy: nothing runs until subscribed. */
@Repository
public class ProductRepository {

    private static final String COLUMNS = "id, sku, name, price, active, version";

    private final DatabaseClient db;

    public ProductRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Product> insert(Product product) {
        return db.sql("""
                        INSERT INTO products (id, sku, name, price, active)
                        VALUES (:id, :sku, :name, :price, :active)
                        RETURNING""" + " " + COLUMNS)
                .bind("id", product.id())
                .bind("sku", product.sku())
                .bind("name", product.name())
                .bind("price", product.price())
                .bind("active", product.active())
                .map(ProductRepository::toProduct)
                .one();
    }

    public Mono<Product> findById(UUID id) {
        return db.sql("SELECT " + COLUMNS + " FROM products WHERE id = :id")
                .bind("id", id)
                .map(ProductRepository::toProduct)
                .one();
    }

    public Flux<Product> findByIds(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return Flux.empty();
        }
        return db.sql("SELECT " + COLUMNS + " FROM products WHERE id = ANY(:ids)")
                .bind("ids", ids.toArray(UUID[]::new))
                .map(ProductRepository::toProduct)
                .all();
    }

    public Flux<Product> findPage(int limit, long offset) {
        return db.sql("SELECT " + COLUMNS + " FROM products ORDER BY created_at, id LIMIT :limit OFFSET :offset")
                .bind("limit", limit)
                .bind("offset", offset)
                .map(ProductRepository::toProduct)
                .all();
    }

    /**
     * Optimistic update: succeeds only if the stored version equals {@code expectedVersion}.
     * Empty when the product does not exist or the version is stale.
     */
    public Mono<Product> update(UUID id, String name, BigDecimal price, boolean active, long expectedVersion) {
        return db.sql("""
                        UPDATE products
                           SET name = :name, price = :price, active = :active,
                               version = version + 1, updated_at = now()
                         WHERE id = :id AND version = :version
                        RETURNING""" + " " + COLUMNS)
                .bind("id", id)
                .bind("name", name)
                .bind("price", price)
                .bind("active", active)
                .bind("version", expectedVersion)
                .map(ProductRepository::toProduct)
                .one();
    }

    public Mono<Boolean> exists(UUID id) {
        return db.sql("SELECT 1 FROM products WHERE id = :id")
                .bind("id", id)
                .map(row -> Boolean.TRUE)
                .one()
                .defaultIfEmpty(Boolean.FALSE);
    }

    private static Product toProduct(Readable row) {
        return new Product(
                row.get("id", UUID.class),
                row.get("sku", String.class),
                row.get("name", String.class),
                row.get("price", BigDecimal.class),
                Boolean.TRUE.equals(row.get("active", Boolean.class)),
                row.get("version", Long.class));
    }
}
