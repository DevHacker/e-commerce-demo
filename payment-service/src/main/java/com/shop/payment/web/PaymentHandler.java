package com.shop.payment.web;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import com.shop.payment.domain.strategy.PaymentStrategies;
import com.shop.payment.service.PaymentService;
import com.shop.payment.web.PaymentDtos.MethodResponse;
import com.shop.payment.web.PaymentDtos.PaymentResponse;
import reactor.core.publisher.Mono;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/** Read-only endpoints: payments are created by the saga, not by HTTP callers. */
@Component
public class PaymentHandler {

    private final PaymentService service;
    private final PaymentStrategies strategies;

    public PaymentHandler(PaymentService service, PaymentStrategies strategies) {
        this.service = service;
        this.strategies = strategies;
    }

    /** The accepted methods and the detail fields each one needs: it is the strategies describing themselves. */
    public Mono<ServerResponse> methods(ServerRequest request) {
        List<MethodResponse> body = strategies.all().values().stream()
                .sorted(Comparator.comparing(s -> s.method().name()))
                .map(s -> new MethodResponse(s.method().name(), s.requiredDetails().stream().sorted().toList()))
                .toList();
        return ServerResponse.ok().bodyValue(body);
    }

    public Mono<ServerResponse> getByOrder(ServerRequest request) {
        return Mono.fromSupplier(() -> orderId(request))
                .flatMap(service::get)
                .map(PaymentResponse::from)
                .flatMap(body -> ServerResponse.ok().bodyValue(body));
    }

    private static UUID orderId(ServerRequest request) {
        String raw = request.pathVariable("orderId");
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Invalid order id", List.of("'" + raw + "' is not a UUID"));
        }
    }
}
