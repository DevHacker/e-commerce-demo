package com.shop.common.config;

import java.time.Duration;

import io.r2dbc.postgresql.client.SSLMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Externalised R2DBC settings. Every value can be overridden per environment
 * (application.yml, environment variables, config server).
 */
@ConfigurationProperties(prefix = "shop.r2dbc")
public record R2dbcProperties(
        @DefaultValue("localhost") String host,
        @DefaultValue("5432") int port,
        String database,
        String username,
        String password,
        @DefaultValue Pool pool,
        @DefaultValue Postgres postgres) {

    /**
     * Connection pool sizing. Keep {@code maxSize} small: a reactive service needs few
     * connections; scale by adding instances, not by growing the pool.
     */
    public record Pool(
            @DefaultValue("shop-r2dbc-pool") String name,
            @DefaultValue("5") int initialSize,
            @DefaultValue("20") int maxSize,
            @DefaultValue("30m") Duration maxLifeTime,
            @DefaultValue("10m") Duration maxIdleTime,
            @DefaultValue("5s") Duration maxAcquireTime,
            @DefaultValue("5s") Duration maxCreateConnectionTime,
            @DefaultValue("2") int acquireRetry,
            @DefaultValue("30s") Duration backgroundEvictionInterval,
            @DefaultValue("") String validationQuery,
            @DefaultValue("true") boolean warmup) {
    }

    /** Postgres driver options. */
    public record Postgres(
            @DefaultValue("shop-service") String applicationName,
            @DefaultValue("5s") Duration connectTimeout,
            @DefaultValue("30s") Duration statementTimeout,
            @DefaultValue("true") boolean tcpKeepAlive,
            @DefaultValue("true") boolean tcpNoDelay,
            @DefaultValue("256") int preparedStatementCacheQueries,
            @DefaultValue("DISABLE") SSLMode sslMode) {
    }
}
