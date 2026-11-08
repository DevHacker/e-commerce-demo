package com.shop.order.messaging;

import java.util.function.Function;
import java.util.stream.Collectors;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.order.domain.SagaMessages.PaymentCompleted;
import com.shop.order.domain.SagaMessages.PaymentFailed;
import com.shop.order.domain.SagaMessages.PaymentRefunded;
import com.shop.order.domain.SagaMessages.ProductsRejected;
import com.shop.order.domain.SagaMessages.ProductsValidated;
import com.shop.order.domain.SagaMessages.Reply;
import com.shop.order.domain.SagaMessages.StockRejected;
import com.shop.order.domain.SagaMessages.StockReleased;
import com.shop.order.domain.SagaMessages.StockReserved;
import com.shop.order.messaging.ReplyMessages.ProductsRejectedData;
import com.shop.order.messaging.ReplyMessages.ProductsValidatedData;
import com.shop.order.messaging.ReplyMessages.StockRejectedData;
import com.shop.order.service.SagaCoordinator;
import tools.jackson.databind.JsonNode;

/**
 * One tiny function per reply type: parse the envelope, turn the payload into a domain {@link Reply},
 * hand it to the coordinator. Parsing problems are not retryable (dead letter topic).
 */
public final class SagaReplyHandlers {

    private SagaReplyHandlers() {
    }

    public static HandlerRegistry registry(SagaCoordinator coordinator, JsonCodec codec) {
        return HandlerRegistry.builder()
                .on(ReplyMessages.PRODUCTS_VALIDATED, handler(coordinator, codec, ProductsValidatedData.class,
                        data -> new ProductsValidated(data.items().stream().collect(Collectors.toMap(
                                ProductsValidatedData.Priced::productId, ProductsValidatedData.Priced::unitPrice)))))
                .on(ReplyMessages.PRODUCTS_REJECTED, handler(coordinator, codec, ProductsRejectedData.class,
                        data -> new ProductsRejected(data.rejections().stream()
                                .map(r -> r.productId() + ":" + r.reason()).toList())))
                .on(ReplyMessages.STOCK_RESERVED, handler(coordinator, codec, JsonNode.class,
                        data -> new StockReserved()))
                .on(ReplyMessages.STOCK_REJECTED, handler(coordinator, codec, StockRejectedData.class,
                        data -> new StockRejected(data.shortages().stream()
                                .map(s -> s.productId() + ": requested " + s.requested() + ", missing " + s.missing())
                                .toList())))
                .on(ReplyMessages.STOCK_RELEASED, handler(coordinator, codec, JsonNode.class,
                        data -> new StockReleased()))
                .on(ReplyMessages.PAYMENT_COMPLETED, handler(coordinator, codec, ReplyMessages.PaymentCompletedData.class,
                        data -> new PaymentCompleted(data.status())))
                .on(ReplyMessages.PAYMENT_FAILED, handler(coordinator, codec, ReplyMessages.PaymentFailedData.class,
                        data -> new PaymentFailed(data.code(), data.message() == null ? "" : data.message())))
                .on(ReplyMessages.PAYMENT_REFUNDED, handler(coordinator, codec, ReplyMessages.PaymentRefundedData.class,
                        data -> new PaymentRefunded(data.refunded())))
                .build();
    }

    private static <D> Handler handler(SagaCoordinator coordinator, JsonCodec codec, Class<D> dataType,
                                       Function<D, Reply> toReply) {
        return message -> {
            Envelope<D> envelope = message.envelope(codec, dataType);
            if (envelope.sagaId() == null) {
                throw new NonRetryableException(message.type() + " without sagaId");
            }
            Reply reply;
            try {
                reply = toReply.apply(envelope.data());
            } catch (NullPointerException | IllegalStateException e) {
                throw new NonRetryableException(message.type() + " has an unusable payload", e);
            }
            return coordinator.onReply(envelope.sagaId(), message.eventId(), reply);
        };
    }
}
