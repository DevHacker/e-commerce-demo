package com.shop.common.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Flyway runs over JDBC, only at startup. When {@code url}, {@code user} or {@code password}
 * are blank they are derived from {@link R2dbcProperties}.
 */
@ConfigurationProperties(prefix = "shop.flyway")
public record FlywayProperties(
        @DefaultValue("true") boolean enabled,
        String url,
        String user,
        String password,
        @DefaultValue({"classpath:db/common", "classpath:db/migration"}) List<String> locations,
        @DefaultValue("10") int connectRetries) {
}
