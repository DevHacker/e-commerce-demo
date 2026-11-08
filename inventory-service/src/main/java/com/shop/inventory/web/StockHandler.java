package com.shop.inventory.web;

import java.util.List;
import java.util.UUID;

import com.shop.inventory.service.StockAdminService;
import com.shop.inventory.web.StockDtos.SetStockRequest;
import com.shop.inventory.web.StockDtos.StockResponse;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers of the admin endpoints: {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class StockHandler {

    private final StockAdminService service;

    public StockHandler(StockAdminService service) {
        this.service = service;
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> productId(request))
                .flatMap(id -> service.get(id).map(overview -> StockResponse.from(id, overview)))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> set(ServerRequest request) {
        return Mono.fromSupplier(() -> productId(request))
                .flatMap(id -> request.bodyToMono(SetStockRequest.class)
                        .switchIfEmpty(Mono.error(
                                () -> new InvalidRequestException("Request body is required", List.of())))
                        .flatMap(body -> {
                            List<String> violations = RequestValidation.check(body);
                            return violations.isEmpty()
                                    ? service.set(id, body.quantity(), body.bucketCount())
                                    : Mono.error(new InvalidRequestException("Invalid request", violations));
                        })
                        .map(overview -> StockResponse.from(id, overview)))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    private static UUID productId(ServerRequest request) {
        String raw = request.pathVariable("productId");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid product id", List.of("'" + raw + "' is not a UUID"));
        }
    }
}
