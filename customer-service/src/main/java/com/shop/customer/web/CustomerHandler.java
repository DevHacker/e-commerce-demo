package com.shop.customer.web;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.shop.customer.service.CustomerService;
import com.shop.customer.web.CustomerDtos.CreateCustomerRequest;
import com.shop.customer.web.CustomerDtos.CustomerResponse;
import com.shop.customer.web.CustomerDtos.PageResponse;
import com.shop.customer.web.CustomerDtos.UpdateCustomerRequest;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers of the functional endpoints: each one is {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class CustomerHandler {

    private final CustomerService service;

    public CustomerHandler(CustomerService service) {
        this.service = service;
    }

    public Mono<ServerResponse> create(ServerRequest request) {
        return request.bodyToMono(CreateCustomerRequest.class)
                .switchIfEmpty(Mono.error(() -> new InvalidRequestException("Request body is required", List.of())))
                .flatMap(body -> validated(body, RequestValidation.check(body)))
                .flatMap(body -> service.create(body.email().trim().toLowerCase(java.util.Locale.ROOT),
                        body.firstName().trim(), body.lastName().trim(), blankToNull(body.phone()), blankToNull(body.address())))
                .map(CustomerResponse::from)
                .flatMap(created -> ServerResponse.created(URI.create("/customers/" + created.id())).bodyValue(created));
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> pathId(request))
                .flatMap(service::get)
                .map(CustomerResponse::from)
                .flatMap(customer -> ServerResponse.ok().bodyValue(customer));
    }

    public Mono<ServerResponse> list(ServerRequest request) {
        return Mono.fromSupplier(() -> new PageParams(
                        intParam(request, "page", 0, 0, Integer.MAX_VALUE / 2),
                        intParam(request, "size", 20, 1, RequestValidation.MAX_PAGE_SIZE)))
                .flatMap(params -> service.list(params.page(), params.size())
                        .map(CustomerResponse::from)
                        .collectList()
                        .map(rows -> new PageResponse<>(
                                rows.size() > params.size() ? rows.subList(0, params.size()) : rows,
                                params.page(), params.size(), rows.size() > params.size())))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> update(ServerRequest request) {
        return Mono.fromSupplier(() -> pathId(request))
                .flatMap(id -> request.bodyToMono(UpdateCustomerRequest.class)
                        .switchIfEmpty(Mono.error(() -> new InvalidRequestException("Request body is required", List.of())))
                        .flatMap(body -> validated(body, RequestValidation.check(body)))
                        .flatMap(body -> service.update(id, body.firstName().trim(), body.lastName().trim(),
                                blankToNull(body.phone()), blankToNull(body.address()), body.active(), body.version())))
                .map(CustomerResponse::from)
                .flatMap(customer -> ServerResponse.ok().bodyValue(customer));
    }

    private record PageParams(int page, int size) {
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
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
            throw new InvalidRequestException("Invalid customer id", List.of("'" + raw + "' is not a UUID"));
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
