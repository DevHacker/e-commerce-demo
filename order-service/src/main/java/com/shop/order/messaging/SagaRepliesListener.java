package com.shop.order.messaging;

import com.shop.common.config.KafkaConsumerConfig;
import com.shop.common.messaging.MessageDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** The only Kafka entry point of the service: hands every reply to the dispatcher. */
@Component
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class SagaRepliesListener {

    private final MessageDispatcher dispatcher;

    public SagaRepliesListener(MessageDispatcher sagaRepliesDispatcher) {
        this.dispatcher = sagaRepliesDispatcher;
    }

    @KafkaListener(topics = ReplyMessages.TOPIC, containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY)
    void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        dispatcher.onMessage(record, acknowledgment);
    }
}
