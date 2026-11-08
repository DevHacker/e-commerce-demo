package com.shop.payment.support;

import org.testcontainers.kafka.KafkaContainer;

/** One Kafka broker (KRaft) shared by every integration test of the JVM. */
public final class KafkaSupport {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static {
        KAFKA.start();
    }

    private KafkaSupport() {
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
