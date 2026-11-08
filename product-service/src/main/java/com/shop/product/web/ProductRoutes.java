package com.shop.product.web;

import java.util.List;

import com.shop.product.domain.DuplicateSkuException;
import com.shop.product.domain.ProductNotFoundException;
import com.shop.product.domain.VersionConflictException;
import com.shop.product.web.ProductDtos.ApiError;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/** The HTTP surface as data: a routing table plus a table of exception-to-status mappings. */
@Configuration(proxyBeanMethods = false)
public class ProductRoutes {

    @Bean
    public RouterFunction<ServerResponse> productRouter(ProductHandler handler) {
        return RouterFunctions.route()
                .path("/products", products -> products
                        .POST("", handler::create)
                        .GET("", handler::list)
                        .GET("/{id}", handler::get)
                        .PUT("/{id}", handler::update))
                .onError(InvalidRequestException.class, (e, r) -> problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e))
                .onError(ProductNotFoundException.class, (e, r) -> problem(HttpStatus.NOT_FOUND, "PRODUCT_NOT_FOUND", e))
                .onError(VersionConflictException.class, (e, r) -> problem(HttpStatus.CONFLICT, "VERSION_CONFLICT", e))
                .onError(DuplicateSkuException.class, (e, r) -> problem(HttpStatus.CONFLICT, "DUPLICATE_SKU", e))
                .build();
    }

    private static Mono<ServerResponse> problem(HttpStatus status, String code, RuntimeException e) {
        List<String> details = e instanceof InvalidRequestException invalid ? invalid.details() : List.of();
        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ApiError(code, e.getMessage(), details));
    }
}
