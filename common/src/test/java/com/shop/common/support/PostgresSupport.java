package com.shop.common.support;

import org.testcontainers.postgresql.PostgreSQLContainer;

/** One PostgreSQL container shared by every integration test of the JVM (stopped by Testcontainers at exit). */
public final class PostgresSupport {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine")
            .withDatabaseName("shop_it")
            .withUsername("shop")
            .withPassword("shop");

    static {
        POSTGRES.start();
    }

    private PostgresSupport() {
    }

    /** {@code shop.r2dbc.*} properties pointing at the container, with a small pool. */
    public static String[] r2dbcProperties() {
        return new String[] {
                "shop.r2dbc.host=" + POSTGRES.getHost(),
                "shop.r2dbc.port=" + POSTGRES.getMappedPort(5432),
                "shop.r2dbc.database=" + POSTGRES.getDatabaseName(),
                "shop.r2dbc.username=" + POSTGRES.getUsername(),
                "shop.r2dbc.password=" + POSTGRES.getPassword(),
                "shop.r2dbc.pool.initial-size=1",
                "shop.r2dbc.pool.max-size=5"
        };
    }
}
