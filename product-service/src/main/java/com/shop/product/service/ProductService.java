package com.shop.product.service;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.product.domain.DuplicateSkuException;
import com.shop.product.domain.Product;
import com.shop.product.domain.ProductNotFoundException;
import com.shop.product.domain.VersionConflictException;
import com.shop.product.repository.ProductRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Service
public class ProductService {

    private final ProductRepository repository;

    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }

    public Mono<Product> create(String sku, String name, BigDecimal price) {
        Product product = new Product(UUID.randomUUID(), sku, name, price, true, 0);
        return repository.insert(product)
                .onErrorMap(DuplicateKeyException.class, e -> new DuplicateSkuException(sku));
    }

    public Mono<Product> get(UUID id) {
        return repository.findById(id).switchIfEmpty(Mono.error(() -> new ProductNotFoundException(id)));
    }

    /** @return one page plus one extra row, so the caller can tell whether a next page exists */
    public Flux<Product> list(int page, int size) {
        return repository.findPage(size + 1, (long) page * size);
    }

    public Mono<Product> update(UUID id, String name, BigDecimal price, boolean active, long expectedVersion) {
        return repository.update(id, name, price, active, expectedVersion)
                .switchIfEmpty(Mono.defer(() -> repository.exists(id)
                        .flatMap(exists -> exists
                                ? Mono.<Product>error(new VersionConflictException(id))
                                : Mono.<Product>error(new ProductNotFoundException(id)))));
    }
}
