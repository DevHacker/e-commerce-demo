package com.shop.order.web;

import java.util.List;

import com.shop.order.domain.OrderExceptions.ConcurrentUpdateException;
import com.shop.order.domain.OrderExceptions.OrderConflictException;
import com.shop.order.domain.OrderExceptions.OrderNotFoundException;
import com.shop.order.web.OrderDtos.ApiError;
import reactor.core.publisher.Mono;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/** The HTTP surface as data: a routing table plus an exception-to-status table. */
@Configuration(proxyBeanMethods = false)
public class OrderRoutes {

    @Bean
    public RouterFunction<ServerResponse> orderRouter(OrderHandler handler) {
        return RouterFunctions.route()
                .path("/orders", orders -> orders
                        .POST("", handler::create)
                        .GET("/{id}", handler::get)
                        .GET("/{id}/history", handler::history)
                        .POST("/{id}/cancel", handler::cancel))
                .onError(InvalidRequestException.class, (e, r) -> problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e))
                .onError(OrderNotFoundException.class, (e, r) -> problem(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", e))
                .onError(OrderConflictException.class, (e, r) -> problem(HttpStatus.CONFLICT, "ORDER_CONFLICT", e))
                .onError(ConcurrentUpdateException.class, (e, r) -> problem(HttpStatus.CONFLICT, "CONCURRENT_UPDATE", e))
                .build();
    }

    private static Mono<ServerResponse> problem(HttpStatus status, String code, RuntimeException e) {
        List<String> details = e instanceof InvalidRequestException invalid ? invalid.details() : List.of();
        return ServerResponse.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ApiError(code, e.getMessage(), details));
    }
}
