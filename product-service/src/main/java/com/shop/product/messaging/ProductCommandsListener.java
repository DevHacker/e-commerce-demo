package com.shop.product.messaging;

import com.shop.common.config.KafkaConsumerConfig;
import com.shop.common.messaging.MessageDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** The only Kafka entry point of the service: hands every record to the dispatcher. */
@Component
@ConditionalOnProperty("shop.kafka.bootstrap-servers")
public class ProductCommandsListener {

    private final MessageDispatcher dispatcher;

    public ProductCommandsListener(MessageDispatcher productCommandsDispatcher) {
        this.dispatcher = productCommandsDispatcher;
    }

    @KafkaListener(topics = ProductMessages.COMMANDS_TOPIC, containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY)
    void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        dispatcher.onMessage(record, acknowledgment);
    }
}
