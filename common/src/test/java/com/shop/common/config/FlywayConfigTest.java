package com.shop.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class FlywayConfigTest {

    private static final Map<String, Object> R2DBC = Map.of(
            "shop.r2dbc.host", "db.internal",
            "shop.r2dbc.port", "5433",
            "shop.r2dbc.database", "order_db",
            "shop.r2dbc.username", "app",
            "shop.r2dbc.password", "secret");

    private static <T> T bind(String prefix, Class<T> type, Map<String, Object> source) {
        return new Binder(new MapConfigurationPropertySource(source)).bind(prefix, type).get();
    }

    private static FlywayProperties flyway(Map<String, Object> extra) {
        // shop.flyway.enabled gives the binder at least one property for this prefix
        Map<String, Object> source = new java.util.HashMap<>(extra);
        source.putIfAbsent("shop.flyway.enabled", "true");
        return bind("shop.flyway", FlywayProperties.class, source);
    }

    @Test
    void derivesJdbcUrlFromR2dbcSettings() {
        R2dbcProperties r2dbc = bind("shop.r2dbc", R2dbcProperties.class, R2DBC);

        assertThat(FlywayConfig.jdbcUrl(flyway(Map.of()), r2dbc))
                .isEqualTo("jdbc:postgresql://db.internal:5433/order_db");
    }

    @Test
    void explicitUrlWins() {
        R2dbcProperties r2dbc = bind("shop.r2dbc", R2dbcProperties.class, R2DBC);
        FlywayProperties flyway = flyway(Map.of("shop.flyway.url", "jdbc:postgresql://admin-host/other"));

        assertThat(FlywayConfig.jdbcUrl(flyway, r2dbc)).isEqualTo("jdbc:postgresql://admin-host/other");
    }

    @Test
    void defaultLocationsCoverCommonAndServiceMigrations() {
        assertThat(flyway(Map.of()).locations())
                .containsExactly("classpath:db/common", "classpath:db/migration");
    }

    @Test
    void userAndPasswordFallBackToR2dbcCredentials() {
        assertThat(FlywayConfig.valueOrDefault(null, "app")).isEqualTo("app");
        assertThat(FlywayConfig.valueOrDefault("  ", "app")).isEqualTo("app");
        assertThat(FlywayConfig.valueOrDefault("migrator", "app")).isEqualTo("migrator");
    }

    @Test
    void disabledFlywayDoesNotTouchTheDatabase() {
        R2dbcProperties r2dbc = bind("shop.r2dbc", R2dbcProperties.class, R2DBC);
        FlywayProperties disabled = flyway(Map.of("shop.flyway.enabled", "false"));

        FlywayConfig.FlywayMigration result = new FlywayConfig().flywayMigration(disabled, r2dbc);

        assertThat(result.executed()).isFalse();
        assertThat(result.migrationsExecuted()).isZero();
    }
}
