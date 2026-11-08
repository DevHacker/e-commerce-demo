package com.shop.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Housekeeping for {@code processed_events} and {@code outbox_events}.
 * Keep {@code inbox} longer than the Kafka topic retention plus the longest expected redelivery window.
 */
@ConfigurationProperties(prefix = "shop.retention")
public record RetentionProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("7d") Duration inbox,
        @DefaultValue("3d") Duration outbox,
        @DefaultValue("1000") int batchSize,
        @DefaultValue("5m") Duration interval) {
}
