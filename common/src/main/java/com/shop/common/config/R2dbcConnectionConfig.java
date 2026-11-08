package com.shop.common.config;

import java.util.Map;
import java.util.Optional;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.pool.PoolMetrics;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ValidationDepth;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.StringUtils;

/**
 * Production-ready R2DBC {@link ConnectionPool} for PostgreSQL.
 *
 * <p>The pool is created only after Flyway has migrated the schema ({@link FlywayConfig}).
 * It is the single {@code ConnectionFactory} bean of the application, so Spring Boot's own
 * R2DBC auto-configuration backs off.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(R2dbcProperties.class)
public class R2dbcConnectionConfig {

    @Bean(destroyMethod = "dispose")
    @DependsOn(FlywayConfig.BEAN_NAME)
    public ConnectionPool connectionPool(R2dbcProperties props) {
        R2dbcProperties.Pool pool = props.pool();

        ConnectionPoolConfiguration.Builder builder = ConnectionPoolConfiguration
                .builder(new PostgresqlConnectionFactory(connectionConfiguration(props)))
                .name(pool.name())
                .initialSize(pool.initialSize())
                .maxSize(pool.maxSize())
                .maxLifeTime(pool.maxLifeTime())
                .maxIdleTime(pool.maxIdleTime())
                .maxAcquireTime(pool.maxAcquireTime())
                .maxCreateConnectionTime(pool.maxCreateConnectionTime())
                .acquireRetry(pool.acquireRetry())
                .backgroundEvictionInterval(pool.backgroundEvictionInterval())
                .validationDepth(ValidationDepth.REMOTE);
        if (StringUtils.hasText(pool.validationQuery())) {
            builder.validationQuery(pool.validationQuery());
        }

        ConnectionPool connectionPool = new ConnectionPool(builder.build());
        if (pool.warmup() && pool.initialSize() > 0) {
            // Fail fast at startup if the database is unreachable.
            connectionPool.warmup().block(pool.maxCreateConnectionTime().multipliedBy(pool.initialSize() + 1L));
        }
        return connectionPool;
    }

    @Bean
    public DatabaseClient databaseClient(ConnectionPool connectionPool) {
        return DatabaseClient.create(connectionPool);
    }

    /** Exposes pool gauges to Micrometer (picked up automatically when Actuator is present). */
    @Bean
    public MeterBinder r2dbcPoolMeterBinder(ConnectionPool connectionPool, R2dbcProperties props) {
        return registry -> {
            Optional<PoolMetrics> metrics = connectionPool.getMetrics();
            if (metrics.isEmpty()) {
                return;
            }
            PoolMetrics m = metrics.get();
            Tags tags = Tags.of("pool", props.pool().name());
            Gauge.builder("r2dbc.pool.acquired", m, PoolMetrics::acquiredSize).tags(tags).register(registry);
            Gauge.builder("r2dbc.pool.allocated", m, PoolMetrics::allocatedSize).tags(tags).register(registry);
            Gauge.builder("r2dbc.pool.idle", m, PoolMetrics::idleSize).tags(tags).register(registry);
            Gauge.builder("r2dbc.pool.pending", m, PoolMetrics::pendingAcquireSize).tags(tags).register(registry);
            Gauge.builder("r2dbc.pool.max.allocated", m, PoolMetrics::getMaxAllocatedSize).tags(tags).register(registry);
        };
    }

    static PostgresqlConnectionConfiguration connectionConfiguration(R2dbcProperties props) {
        R2dbcProperties.Postgres pg = props.postgres();
        return PostgresqlConnectionConfiguration.builder()
                .host(props.host())
                .port(props.port())
                .database(props.database())
                .username(props.username())
                .password(props.password())
                .applicationName(pg.applicationName())
                .connectTimeout(pg.connectTimeout())
                .tcpKeepAlive(pg.tcpKeepAlive())
                .tcpNoDelay(pg.tcpNoDelay())
                .preparedStatementCacheQueries(pg.preparedStatementCacheQueries())
                .sslMode(pg.sslMode())
                .options(Map.of("statement_timeout", Long.toString(pg.statementTimeout().toMillis())))
                .build();
    }
}
