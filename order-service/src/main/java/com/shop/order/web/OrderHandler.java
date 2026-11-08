package com.shop.order.web;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.shop.order.domain.Records.Loaded;
import com.shop.order.domain.SagaMessages.Line;
import com.shop.order.service.OrderService;
import com.shop.order.service.OrderService.Created;
import com.shop.order.service.OrderService.History;
import com.shop.order.web.OrderDtos.AcceptedResponse;
import com.shop.order.web.OrderDtos.CreateOrderRequest;
import com.shop.order.web.OrderDtos.HistoryResponse;
import com.shop.order.web.OrderDtos.ItemResponse;
import com.shop.order.web.OrderDtos.OrderResponse;
import com.shop.order.web.OrderDtos.StatusEntryResponse;
import com.shop.order.web.OrderDtos.StepEntryResponse;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Request handlers: each one is {@code ServerRequest -> Mono<ServerResponse>}. */
@Component
public class OrderHandler {

    static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final OrderService service;
    private final JsonMapper json;

    public OrderHandler(OrderService service, JsonMapper json) {
        this.service = service;
        this.json = json;
    }

    /** 202: the order exists and its saga has started; the final status is reached asynchronously. */
    public Mono<ServerResponse> create(ServerRequest request) {
        String key = request.headers().firstHeader(IDEMPOTENCY_HEADER);
        return request.bodyToMono(CreateOrderRequest.class)
                .switchIfEmpty(Mono.error(() -> new InvalidRequestException("Request body is required", List.of())))
                .flatMap(body -> {
                    List<String> violations = RequestValidation.check(key, body);
                    if (!violations.isEmpty()) {
                        return Mono.error(new InvalidRequestException("Invalid request", violations));
                    }
                    List<Line> items = body.items().stream()
                            .map(i -> new Line(i.productId(), i.quantity())).toList();
                    Map<String, String> details = body.payment().details() == null
                            ? Map.of() : Map.copyOf(body.payment().details());
                    return service.create(key.trim(), body.customerRef().trim(), items,
                            RequestValidation.normalizedMethod(body.payment()), details);
                })
                .flatMap(created -> accepted(created));
    }

    public Mono<ServerResponse> get(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(service::get)
                .map(this::toResponse)
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> history(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(id -> service.history(id).map(h -> toResponse(id, h)))
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    public Mono<ServerResponse> cancel(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(service::cancel)
                .flatMap(created -> accepted(created));
    }

    private static Mono<ServerResponse> accepted(Created created) {
        return ServerResponse.accepted()
                .location(URI.create("/orders/" + created.orderId()))
                .bodyValue(new AcceptedResponse(created.orderId(), created.status()));
    }

    private OrderResponse toResponse(Loaded loaded) {
        return new OrderResponse(
                loaded.order().id(), loaded.order().customerRef(), loaded.order().status(), loaded.order().total(),
                loaded.order().paymentMethod(),
                loaded.items().stream().map(i -> new ItemResponse(i.productId(), i.quantity(), i.unitPrice())).toList(),
                loaded.order().version());
    }

    private HistoryResponse toResponse(UUID orderId, History history) {
        return new HistoryResponse(
                orderId,
                history.status().stream()
                        .map(r -> new StatusEntryResponse(r.from(), r.to(), r.reason(), r.triggerMessageId(), r.occurredAt()))
                        .toList(),
                history.steps().stream()
                        .map(r -> new StepEntryResponse(r.step(), r.direction(), r.outcome(), r.messageId(),
                                r.detail() == null ? null : parse(r.detail()), r.occurredAt()))
                        .toList());
    }

    private JsonNode parse(String text) {
        return json.readTree(text);
    }

    private static UUID orderId(ServerRequest request) {
        String raw = request.pathVariable("id");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid order id", List.of("'" + raw + "' is not a UUID"));
        }
    }
}
