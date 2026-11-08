package com.shop.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import io.r2dbc.postgresql.client.SSLMode;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class R2dbcPropertiesTest {

    private static R2dbcProperties bind(Map<String, Object> source) {
        return new Binder(new MapConfigurationPropertySource(source))
                .bind("shop.r2dbc", R2dbcProperties.class)
                .get();
    }

    @Test
    void appliesProductionDefaults() {
        R2dbcProperties props = bind(Map.of(
                "shop.r2dbc.database", "shop",
                "shop.r2dbc.username", "app",
                "shop.r2dbc.password", "secret"));

        assertThat(props.host()).isEqualTo("localhost");
        assertThat(props.port()).isEqualTo(5432);
        assertThat(props.pool().initialSize()).isEqualTo(5);
        assertThat(props.pool().maxSize()).isEqualTo(20);
        assertThat(props.pool().maxAcquireTime()).isEqualTo(Duration.ofSeconds(5));
        assertThat(props.pool().maxLifeTime()).isEqualTo(Duration.ofMinutes(30));
        assertThat(props.pool().acquireRetry()).isEqualTo(2);
        assertThat(props.pool().validationQuery()).isBlank();
        assertThat(props.pool().warmup()).isTrue();
        assertThat(props.postgres().connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(props.postgres().statementTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(props.postgres().tcpKeepAlive()).isTrue();
        assertThat(props.postgres().sslMode()).isEqualTo(SSLMode.DISABLE);
    }

    @Test
    void everyDefaultCanBeOverridden() {
        R2dbcProperties props = bind(Map.of(
                "shop.r2dbc.database", "shop",
                "shop.r2dbc.host", "db.internal",
                "shop.r2dbc.pool.max-size", "50",
                "shop.r2dbc.pool.max-acquire-time", "2s",
                "shop.r2dbc.postgres.ssl-mode", "verify-full",
                "shop.r2dbc.postgres.statement-timeout", "10s"));

        assertThat(props.host()).isEqualTo("db.internal");
        assertThat(props.pool().maxSize()).isEqualTo(50);
        assertThat(props.pool().maxAcquireTime()).isEqualTo(Duration.ofSeconds(2));
        assertThat(props.postgres().sslMode()).isEqualTo(SSLMode.VERIFY_FULL);
        assertThat(props.postgres().statementTimeout()).isEqualTo(Duration.ofSeconds(10));
    }
}
