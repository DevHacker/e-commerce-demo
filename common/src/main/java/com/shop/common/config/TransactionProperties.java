package com.shop.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "shop.tx")
public record TransactionProperties(@DefaultValue("10s") Duration timeout) {
}
