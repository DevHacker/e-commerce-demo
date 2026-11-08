package com.shop.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Kafka consumer settings ({@code shop.kafka.*}). The consumer configuration is only active when
 * {@code shop.kafka.bootstrap-servers} is set.
 */
@ConfigurationProperties(prefix = "shop.kafka")
public record KafkaConsumerProperties(
        String bootstrapServers,
        String groupId,
        @DefaultValue("3") int concurrency,
        @DefaultValue("100") int maxPollRecords,
        @DefaultValue("5m") Duration maxPollInterval,
        @DefaultValue("45s") Duration sessionTimeout,
        @DefaultValue Retry retry) {

    /**
     * Retry before dead-lettering. Retries block the partition's consumer thread, so keep the
     * total well below {@code maxPollInterval}.
     */
    public record Retry(
            @DefaultValue("500ms") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("10s") Duration maxInterval,
            @DefaultValue("5") int maxAttempts) {
    }
}
