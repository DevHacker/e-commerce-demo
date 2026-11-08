package com.shop.order.service;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** {@code shop.order.*}: how long a saga may stay without progress, and how the sweeper polls. */
@ConfigurationProperties(prefix = "shop.order")
public record OrderProperties(
        @DefaultValue("2m") Duration sagaTimeout,
        @DefaultValue("30s") Duration sweepInterval,
        @DefaultValue("100") int sweepBatchSize,
        @DefaultValue("true") boolean sweeperEnabled) {
}
