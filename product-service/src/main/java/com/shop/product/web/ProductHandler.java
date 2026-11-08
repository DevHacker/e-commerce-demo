package com.shop.product.web;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.shop.product.service.ProductService;
import com.shop.product.web.ProductDtos.CreateProductRequest;
import com.shop.product.web.ProductDtos.PageResponse;
import com.shop.product.web.ProductDtos.ProductResponse;
import com.shop.product.web.ProductDtos.UpdateProductRequest;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers of the functional endpoints: each one is {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class ProductHandler {

    private final ProductService service;

    public ProductHandler(ProductService service) {
        this.service = service;
    }

    public Mono<ServerResponse> create(ServerRequest request) {
        return request.bodyToMono(CreateProductRequest.class)
                .switchIfEmpty(Mono.error(() -> new InvalidRequestException("Request body is required", List.of())))
                .flatMap(body -> validated(body, RequestValidation.check(body)))
                .flatMap(body -> service.create(body.sku().trim(), body.name().trim(), body.price()))
                .map(ProductResponse::from)
                .flatMap(created -> ServerResponse.created(URI.create("/products/" + created.id()))
                        .bodyValue(created));
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> pathId(request))
                .flatMap(service::get)
                .map(ProductResponse::from)
                .flatMap(product -> ServerResponse.ok().bodyValue(product));
    }

    public Mono<ServerResponse> list(ServerRequest request) {
        return Mono.fromSupplier(() -> new PageParams(
                        intParam(request, "page", 0, 0, Integer.MAX_VALUE / 2),
                        intParam(request, "size", 20, 1, RequestValidation.MAX_PAGE_SIZE)))
                .flatMap(params -> service.list(params.page(), params.size())
                        .map(ProductResponse::from)
                        .collectList()
                        .map(rows -> new PageResponse<>(
                                rows.size() > params.size()
                                        ? rows.subList(0, params.size())
                                        : rows,
                                params.page(),
                                params.size(),
                                rows.size() > params.size())))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> update(ServerRequest request) {
        return Mono.fromSupplier(() -> pathId(request))
                .flatMap(id -> request.bodyToMono(UpdateProductRequest.class)
                        .switchIfEmpty(Mono.error(
                                () -> new InvalidRequestException("Request body is required", List.of())))
                        .flatMap(body -> validated(body, RequestValidation.check(body)))
                        .flatMap(body -> service.update(
                                id, body.name().trim(), body.price(), body.active(), body.version())))
                .map(ProductResponse::from)
                .flatMap(product -> ServerResponse.ok().bodyValue(product));
    }

    private record PageParams(int page, int size) {
    }

    private static <T> Mono<T> validated(T body, List<String> violations) {
        return violations.isEmpty()
                ? Mono.just(body)
                : Mono.error(new InvalidRequestException("Invalid request", violations));
    }

    private static UUID pathId(ServerRequest request) {
        String raw = request.pathVariable("id");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid product id", List.of("'" + raw + "' is not a UUID"));
        }
    }

    private static int intParam(ServerRequest request, String name, int defaultValue, int min, int max) {
        return request.queryParam(name).map(raw -> {
            try {
                int value = Integer.parseInt(raw);
                if (value < min || value > max) {
                    throw new NumberFormatException();
                }
                return value;
            } catch (NumberFormatException e) {
                throw new InvalidRequestException("Invalid query parameter",
                        List.of(name + " must be an integer between " + min + " and " + max));
            }
        }).orElse(defaultValue);
    }
}
