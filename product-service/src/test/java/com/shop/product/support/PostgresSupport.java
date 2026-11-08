package com.shop.product.support;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.postgresql.PostgreSQLContainer;

/** One PostgreSQL container shared by every integration test of the JVM (stopped by Testcontainers at exit). */
public final class PostgresSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("product_it")
            .withUsername("shop")
            .withPassword("shop");

    static {
        POSTGRES.start();
    }

    private PostgresSupport() {
    }

    /** {@code shop.r2dbc.*} properties pointing at the container, with a small pool. */
    public static Map<String, String> r2dbcProperties() {
        return Stream.of(
                        new String[] {"shop.r2dbc.host", POSTGRES.getHost()},
                        new String[] {"shop.r2dbc.port", String.valueOf(POSTGRES.getMappedPort(5432))},
                        new String[] {"shop.r2dbc.database", POSTGRES.getDatabaseName()},
                        new String[] {"shop.r2dbc.username", POSTGRES.getUsername()},
                        new String[] {"shop.r2dbc.password", POSTGRES.getPassword()},
                        new String[] {"shop.r2dbc.pool.initial-size", "1"},
                        new String[] {"shop.r2dbc.pool.max-size", "5"})
                .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }
}
