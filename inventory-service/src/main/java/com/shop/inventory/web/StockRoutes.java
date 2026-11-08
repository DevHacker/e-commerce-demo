package com.shop.inventory.web;

import java.util.List;

import com.shop.inventory.domain.StockConflictException;
import com.shop.inventory.domain.StockNotFoundException;
import com.shop.inventory.web.StockDtos.ApiError;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/** The admin HTTP surface as data: a routing table plus an exception-to-status table. */
@Configuration(proxyBeanMethods = false)
public class StockRoutes {

    @Bean
    public RouterFunction<ServerResponse> stockRouter(StockHandler handler) {
        return RouterFunctions.route()
                .path("/stock", stock -> stock
                        .GET("/{productId}", handler::get)
                        .PUT("/{productId}", handler::set))
                .onError(InvalidRequestException.class, (e, r) -> problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e))
                .onError(StockNotFoundException.class, (e, r) -> problem(HttpStatus.NOT_FOUND, "STOCK_NOT_FOUND", e))
                .onError(StockConflictException.class, (e, r) -> problem(HttpStatus.CONFLICT, "STOCK_CONFLICT", e))
                .build();
    }

    private static Mono<ServerResponse> problem(HttpStatus status, String code, RuntimeException e) {
        List<String> details = e instanceof InvalidRequestException invalid ? invalid.details() : List.of();
        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ApiError(code, e.getMessage(), details));
    }
}
