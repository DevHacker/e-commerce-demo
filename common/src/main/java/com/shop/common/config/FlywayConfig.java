package com.shop.common.config;

import org.flywaydb.core.Flyway;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Runs Flyway migrations at startup through a short-lived JDBC connection (Flyway has no R2DBC
 * support). {@link R2dbcConnectionConfig} depends on this bean, so the R2DBC pool opens only
 * after the schema is ready. The runtime stays fully reactive.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({FlywayProperties.class, R2dbcProperties.class})
public class FlywayConfig {

    public static final String BEAN_NAME = "flywayMigration";

    @Bean(name = BEAN_NAME)
    public FlywayMigration flywayMigration(FlywayProperties flyway, R2dbcProperties r2dbc) {
        if (!flyway.enabled()) {
            return new FlywayMigration(0, false);
        }
        Flyway engine = Flyway.configure()
                .dataSource(
                        jdbcUrl(flyway, r2dbc),
                        valueOrDefault(flyway.user(), r2dbc.username()),
                        valueOrDefault(flyway.password(), r2dbc.password()))
                .locations(flyway.locations().toArray(String[]::new))
                .connectRetries(flyway.connectRetries())
                .load();
        return new FlywayMigration(engine.migrate().migrationsExecuted, true);
    }

    static String jdbcUrl(FlywayProperties flyway, R2dbcProperties r2dbc) {
        if (StringUtils.hasText(flyway.url())) {
            return flyway.url();
        }
        return "jdbc:postgresql://%s:%d/%s".formatted(r2dbc.host(), r2dbc.port(), r2dbc.database());
    }

    static String valueOrDefault(String value, String fallback) {
        return StringUtils.hasText(value) ? value : fallback;
    }

    /** Marker bean describing what the startup migration did. */
    public record FlywayMigration(int migrationsExecuted, boolean executed) {
    }
}
