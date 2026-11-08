package com.shop.product.messaging;

import java.util.List;
import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.product.domain.Product;
import com.shop.product.domain.ProductValidation;
import com.shop.product.messaging.ProductMessages.ProductsRejectedData;
import com.shop.product.messaging.ProductMessages.ProductsValidatedData;
import com.shop.product.messaging.ProductMessages.ValidateProductsData;
import com.shop.product.repository.ProductRepository;
import reactor.core.publisher.Mono;

/**
 * Saga step "ValidateProducts": read the catalogue, decide with the pure
 * {@link ProductValidation#validate}, and answer through the outbox on the topic named by the
 * command's {@code replyTo}. Read-only on the catalogue, so a rejection needs no compensation.
 * Runs inside the dispatcher's transaction (inbox row + outbox row commit together).
 */
public class ValidateProductsHandler implements Handler {

    static final String AGGREGATE_TYPE = "Order";

    private final ProductRepository products;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ValidateProductsHandler(ProductRepository products, OutboxWriter outbox, JsonCodec codec) {
        this.products = products;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ValidateProductsData> command = message.envelope(codec, ValidateProductsData.class);
        List<UUID> requested = requestedIds(command);
        String orderId = message.key() != null ? message.key() : command.sagaId().toString();

        return products.findByIds(requested)
                .collectMap(Product::id)
                .map(catalogue -> ProductValidation.validate(requested, catalogue))
                .flatMap(result -> reply(orderId, command, result))
                .then();
    }

    private Mono<?> reply(String orderId, Envelope<ValidateProductsData> command, ProductValidation.Result result) {
        return switch (result) {
            case ProductValidation.Validated ok -> outbox.write(
                    command.replyTo(), AGGREGATE_TYPE, orderId, ProductMessages.PRODUCTS_VALIDATED,
                    command.withData(new ProductsValidatedData(ok.lines())));
            case ProductValidation.Rejected ko -> outbox.write(
                    command.replyTo(), AGGREGATE_TYPE, orderId, ProductMessages.PRODUCTS_REJECTED,
                    command.withData(new ProductsRejectedData(ko.rejections())));
        };
    }

    private static List<UUID> requestedIds(Envelope<ValidateProductsData> command) {
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ValidateProducts without replyTo");
        }
        ValidateProductsData data = command.data();
        if (data == null || data.items() == null || data.items().isEmpty()) {
            throw new NonRetryableException("ValidateProducts without items");
        }
        return data.items().stream().map(ValidateProductsData.Item::productId).toList();
    }
}
