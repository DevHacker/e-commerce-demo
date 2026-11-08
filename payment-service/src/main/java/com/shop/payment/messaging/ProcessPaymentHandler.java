package com.shop.payment.messaging;

import java.util.Map;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.Handler;
import com.shop.common.messaging.InboundMessage;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.payment.messaging.PaymentMessages.PaymentCompletedData;
import com.shop.payment.messaging.PaymentMessages.PaymentFailedData;
import com.shop.payment.messaging.PaymentMessages.ProcessPaymentData;
import com.shop.payment.service.PaymentService;
import com.shop.payment.service.PaymentService.Completed;
import com.shop.payment.service.PaymentService.Failed;
import reactor.core.publisher.Mono;

/** Saga step "ProcessPayment": charges the order with its payment method and answers PaymentCompleted or PaymentFailed. */
public class ProcessPaymentHandler implements Handler {

    static final String AGGREGATE_TYPE = "Order";

    private final PaymentService payments;
    private final OutboxWriter outbox;
    private final JsonCodec codec;

    public ProcessPaymentHandler(PaymentService payments, OutboxWriter outbox, JsonCodec codec) {
        this.payments = payments;
        this.outbox = outbox;
        this.codec = codec;
    }

    @Override
    public Mono<Void> apply(InboundMessage message) {
        Envelope<ProcessPaymentData> command = message.envelope(codec, ProcessPaymentData.class);
        if (command.sagaId() == null) {
            throw new NonRetryableException("ProcessPayment without sagaId");
        }
        if (command.replyTo() == null || command.replyTo().isBlank()) {
            throw new NonRetryableException("ProcessPayment without replyTo");
        }
        ProcessPaymentData data = command.data();
        if (data == null || data.amount() == null) {
            throw new NonRetryableException("ProcessPayment without amount");
        }
        Map<String, String> details = data.details() == null ? Map.of() : data.details();
        String key = message.key() != null ? message.key() : command.sagaId().toString();

        return payments.charge(command.sagaId(), data.method(), data.amount(), details)
                .flatMap(outcome -> switch (outcome) {
                    case Completed ok -> outbox.write(command.replyTo(), AGGREGATE_TYPE, key,
                            PaymentMessages.PAYMENT_COMPLETED,
                            command.withData(new PaymentCompletedData(ok.payment().id(), ok.payment().method(),
                                    ok.payment().status().name(), ok.payment().reference())));
                    case Failed ko -> outbox.write(command.replyTo(), AGGREGATE_TYPE, key,
                            PaymentMessages.PAYMENT_FAILED, command.withData(new PaymentFailedData(ko.code(), ko.message())));
                })
                .then();
    }
}
