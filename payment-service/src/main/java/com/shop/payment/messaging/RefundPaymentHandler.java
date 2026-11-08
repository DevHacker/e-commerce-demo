package com.shop.payment.messaging;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.payment.messaging.PaymentMessages.PaymentRefundedData;
import com.shop.payment.messaging.PaymentMessages.RefundPaymentData;
import com.shop.payment.service.PaymentService;
import reactor.core.publisher.Mono;

/**
 * Saga compensation "RefundPayment". Always answers PaymentRefunded, with {@code refunded=false} when there was
 * nothing to give back (never charged, charge failed, cash not collected, already refunded), so the saga can always finish.
 */
public class RefundPaymentHandler implements Handler {

    private final PaymentService payments;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public RefundPaymentHandler(PaymentService payments, OutboxWriter outbox, JsonCodec codec) {
        this.payments = payments;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<RefundPaymentData> command = message.envelope(codec, RefundPaymentData.class);
        if (command.sagaId() == null) {
            throw new NonRetryableException("RefundPayment without sagaId");
        }
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("RefundPayment without replyTo");
        }
        String key = message.key() != null ? message.key() : command.sagaId().toString();

        return payments.refund(command.sagaId())
                .flatMap(outcome -> outbox.write(command.replyTo(), ProcessPaymentHandler.AGGREGATE_TYPE, key,
                        PaymentMessages.PAYMENT_REFUNDED,
                        command.withData(new PaymentRefundedData(outcome.refunded(), outcome.paymentId()))))
                .then();
    }
}
