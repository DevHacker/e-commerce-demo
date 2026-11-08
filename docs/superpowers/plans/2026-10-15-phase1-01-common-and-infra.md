# Phase 1, plan 1: `common` library and local infrastructure

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and publish `shop-common` (reactive PostgreSQL pool, transactions, Flyway, transactional outbox, idempotent inbox, Spring Kafka consumer pipeline) and the local infrastructure (PostgreSQL 18 x3, Kafka, Debezium Connect) that every service of phase 1 builds on.

**Architecture:** `shop-common` is a standalone Maven library exposed as a Spring Boot auto-configuration, installed with `mvn install` to `~/.m2` now and published to Artifactory later. Services depend on it by version and write no infrastructure configuration. Producers never talk to Kafka: they insert into `outbox_events` in the business transaction and Debezium publishes. Consumers run through a Spring Kafka listener container (manual ack after the database commit) into a functional `Handler` registry with an inbox for idempotence.

**Tech Stack:** Java 21, Spring Boot 4.1.1 (Framework 7, Jackson 3), R2DBC (r2dbc-postgresql 1.1.2, r2dbc-pool 1.0.2), Flyway 12.4.0 (JDBC, startup only), Spring Kafka 4.1.1 (kafka-clients 4.2.1), Micrometer, PostgreSQL 18, Debezium 3.7.0.Final, Testcontainers 2.0.5. All library versions are managed by the Spring Boot BOM; none is pinned by hand.

**Spec:** `docs/superpowers/specs/2026-10-15-ecommerce-phase1-design.md`

**This is plan 1 of 5.** Next: plan 2 Product service, plan 3 Inventory service, plan 4 Order service (saga orchestrator), plan 5 end-to-end tests. Each plan produces working, testable software.

## Verification status (read before starting)

The code in this plan was compiled and tested before being written here, in a scratch copy:

- `mvn test`: 24 unit tests green.
- Integration tests: 8 green, run against a real **PostgreSQL 16** and an **embedded Kafka (KRaft) broker**, because the authoring sandbox had no Docker daemon. They cover: Flyway before pool, outbox/inbox commit and rollback, idempotence, batch purge, and the full consumer flow (dispatch, duplicate, 3 deliveries then dead letter, non-retryable, progress after a dead letter).
- `mvn install`: jar, sources and javadoc jars build and install.

**Not executed yet, so Tasks 11 and 12 are the first real run:** the Testcontainers wiring (`postgres:18-alpine`, `apache/kafka:4.1.0`), PostgreSQL 18 itself, and everything in `infra/` (image tags, Debezium EventRouter settings). If a step there fails, fix it before moving on; the expected output of each step says what success looks like.

## File structure

```
ecommerce/
  .gitignore
  common/
    pom.xml
    README.md
    src/main/java/com/shop/common/
      config/    R2dbcProperties, R2dbcConnectionConfig,        <- connection factory + pool (own file)
                 TransactionProperties, TransactionConfig,      <- TransactionalOperator (own file)
                 FlywayProperties, FlywayConfig,
                 KafkaConsumerProperties, KafkaConsumerConfig,
                 RetentionProperties, RetentionConfig,
                 MessagingConfig, CommonAutoConfiguration
      messaging/ Envelope, JsonCodec, InboundMessage, Handler, HandlerRegistry,
                 MessageDispatcher, NonRetryableException
      outbox/    OutboxWriter
      inbox/     Inbox, RetentionPurger
    src/main/resources/
      META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
      db/common/V1__outbox_and_inbox.sql
    src/test/java/com/shop/common/   unit tests, *IT integration tests, support/
  infra/
    docker-compose.yml
    debezium/{product,order,inventory}-outbox.json, smoke/smoke-outbox.json
    scripts/{register-connectors.sh, smoke-outbox.sh}
```

Conventions: unit tests are `*Test` (Maven Surefire, no Docker). Integration tests are `*IT` (Maven Failsafe, Docker needed). All commands run from the `ecommerce/` directory unless stated.

---

### Task 1: Scaffold the `common` library
**Files:**
- Create: `.gitignore`
- Create: `common/pom.xml`

- [ ] **Step 1: Create the root `.gitignore`**

Create `.gitignore`:

```text
target/
*.iml
.idea/
.vscode/
*.log
.DS_Store
```

- [ ] **Step 2: Create the POM**

Create `common/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.1</version>
        <relativePath/>
    </parent>

    <groupId>com.shop</groupId>
    <artifactId>shop-common</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>jar</packaging>
    <name>shop-common</name>
    <description>Shared infrastructure for the shop microservices: R2DBC, transactions, Flyway, outbox, inbox, Kafka consumer.</description>

    <properties>
        <java.version>21</java.version>
        <!-- Placeholder: override with -Dartifactory.url=... when Artifactory is available -->
        <artifactory.url>https://artifactory.invalid/artifactory</artifactory.url>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-autoconfigure</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-r2dbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-tx</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>r2dbc-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>io.r2dbc</groupId>
            <artifactId>r2dbc-pool</artifactId>
        </dependency>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>tools.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-source-plugin</artifactId>
                <executions>
                    <execution>
                        <id>attach-sources</id>
                        <goals>
                            <goal>jar-no-fork</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-javadoc-plugin</artifactId>
                <configuration>
                    <doclint>none</doclint>
                    <failOnError>false</failOnError>
                </configuration>
                <executions>
                    <execution>
                        <id>attach-javadocs</id>
                        <goals>
                            <goal>jar</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <!-- Unit tests: *Test (surefire, no Docker). Integration tests: *IT (failsafe, needs Docker). -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <configuration>
                    <includes>
                        <include>**/*IT.java</include>
                    </includes>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

    <distributionManagement>
        <repository>
            <id>artifactory-releases</id>
            <url>${artifactory.url}/libs-release-local</url>
        </repository>
        <snapshotRepository>
            <id>artifactory-snapshots</id>
            <url>${artifactory.url}/libs-snapshot-local</url>
        </snapshotRepository>
    </distributionManagement>
</project>
```


- [ ] **Step 3: Check that every dependency resolves (names verified against Spring Boot 4.1.1)**

```bash
cd common && mvn -B -q dependency:resolve
```

Expected: no output, exit code 0. If an artifact is reported missing, stop: the BOM changed, do not pin a version by hand.

- [ ] **Step 4: Commit**

```bash
git add .gitignore common/pom.xml
git commit -m "build(common): scaffold shop-common library"
```

### Task 2: R2DBC properties (pool and driver defaults)
**Files:**
- Create: `common/src/test/java/com/shop/common/config/R2dbcPropertiesTest.java`
- Create: `common/src/main/java/com/shop/common/config/R2dbcProperties.java`

- [ ] **Step 1: Write the failing test**

Create `common/src/test/java/com/shop/common/config/R2dbcPropertiesTest.java`:

```java
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
```


- [ ] **Step 2: Run it and see it fail**

```bash
cd common && mvn -B -q test -Dtest=R2dbcPropertiesTest
```

Expected: `COMPILATION ERROR` with `cannot find symbol ... class R2dbcProperties`.

- [ ] **Step 3: Implement**

Create `common/src/main/java/com/shop/common/config/R2dbcProperties.java`:

```java
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
```


- [ ] **Step 4: Run it and see it pass**

```bash
cd common && mvn -B test -Dtest=R2dbcPropertiesTest
```

Expected: `Tests run: 2, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
git add common
git commit -m "feat(common): externalised R2DBC pool and driver properties"
```

### Task 3: Flyway: shared migration and startup runner
**Files:**
- Create: `common/src/main/resources/db/common/V1__outbox_and_inbox.sql`
- Create: `common/src/test/java/com/shop/common/config/FlywayConfigTest.java`
- Create: `common/src/main/java/com/shop/common/config/FlywayProperties.java`
- Create: `common/src/main/java/com/shop/common/config/FlywayConfig.java`

- [ ] **Step 1: Create the shared migration (the schema you specified, plus time indexes for the purge)**

Create `common/src/main/resources/db/common/V1__outbox_and_inbox.sql`:

```sql
-- Shared by every service database: inbox (idempotent consumers) and transactional outbox (Debezium CDC).

CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_processed_events_processed_at ON processed_events (processed_at);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    aggregatetype VARCHAR(100) NOT NULL,
    aggregateid VARCHAR(255) NOT NULL,
    type VARCHAR(150) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_events_occurred_at ON outbox_events (occurred_at);
```


- [ ] **Step 2: Write the failing test**

Create `common/src/test/java/com/shop/common/config/FlywayConfigTest.java`:

```java
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
```


- [ ] **Step 3: Run it and see it fail**

```bash
cd common && mvn -B -q test -Dtest=FlywayConfigTest
```

Expected: `COMPILATION ERROR` with `cannot find symbol ... FlywayProperties`.

- [ ] **Step 4: Implement the properties**

Create `common/src/main/java/com/shop/common/config/FlywayProperties.java`:

```java
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
```


- [ ] **Step 5: Implement the runner. It uses Flyway's own JDBC connection (Flyway has no R2DBC support), derives the JDBC URL from the R2DBC settings, and exposes a marker bean that the R2DBC pool depends on**

Create `common/src/main/java/com/shop/common/config/FlywayConfig.java`:

```java
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
```


- [ ] **Step 6: Run it and see it pass**

```bash
cd common && mvn -B test -Dtest=FlywayConfigTest
```

Expected: `Tests run: 5, Failures: 0, Errors: 0`.

- [ ] **Step 7: Commit**

```bash
git add common
git commit -m "feat(common): Flyway startup migration with shared outbox and inbox schema"
```

### Task 4: Connection pool and transaction configuration (two separate files)
**Files:**
- Create: `common/src/main/java/com/shop/common/config/R2dbcConnectionConfig.java`
- Create: `common/src/main/java/com/shop/common/config/TransactionProperties.java`
- Create: `common/src/main/java/com/shop/common/config/TransactionConfig.java`

- [ ] **Step 1: Create the ConnectionFactory and pool configuration. Notes: the pool bean depends on the Flyway bean, warms up at startup (fails fast if the database is down), validates connections remotely, and is the only `ConnectionFactory` bean so Spring Boot's own R2DBC auto-configuration backs off. Metrics are exposed as Micrometer gauges because r2dbc-pool 1.0.2 ships no Micrometer recorder**

Create `common/src/main/java/com/shop/common/config/R2dbcConnectionConfig.java`:

```java
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
```


- [ ] **Step 2: Create the TransactionalOperator configuration in its own file**

Create `common/src/main/java/com/shop/common/config/TransactionProperties.java`:

```java
package com.shop.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "shop.tx")
public record TransactionProperties(@DefaultValue("10s") Duration timeout) {
}
```


Create `common/src/main/java/com/shop/common/config/TransactionConfig.java`:

```java
package com.shop.common.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * Reactive transaction support, kept apart from the connection configuration.
 * Business code wraps a {@code Mono}/{@code Flux} with
 * {@link TransactionalOperator#transactional(reactor.core.publisher.Mono)}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TransactionProperties.class)
public class TransactionConfig {

    @Bean
    public ReactiveTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }

    @Bean
    public TransactionalOperator transactionalOperator(ReactiveTransactionManager transactionManager,
                                                       TransactionProperties props) {
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        definition.setTimeout((int) props.timeout().toSeconds());
        return TransactionalOperator.create(transactionManager, definition);
    }
}
```


- [ ] **Step 3: Compile**

```bash
cd common && mvn -B -q compile
```

Expected: no output, exit code 0. These two configurations need a database, so they are verified by `PostgresInfrastructureIT` in Task 10, not by a unit test.

- [ ] **Step 4: Commit**

```bash
git add common
git commit -m "feat(common): production-ready R2DBC pool and TransactionalOperator configs"
```

### Task 5: Messaging value types and registry
**Files:**
- Create: `common/src/main/java/com/shop/common/messaging/{NonRetryableException,Envelope,JsonCodec,InboundMessage,Handler,HandlerRegistry}.java`
- Create: `common/src/test/java/com/shop/common/messaging/{JsonCodecTest,InboundMessageTest,HandlerRegistryTest}.java`

- [ ] **Step 1: Write the failing tests**

Create `common/src/test/java/com/shop/common/messaging/JsonCodecTest.java`:

```java
package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class JsonCodecTest {

    record Ping(String name, int count) {
    }

    private final JsonCodec codec = new JsonCodec(JsonMapper.builder().build());

    @Test
    void roundTripsAnEnvelope() {
        Envelope<Ping> original = Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "order.saga.replies",
                new Ping("a", 2));

        Envelope<Ping> parsed = codec.readEnvelope(codec.write(original), Ping.class);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void withDataKeepsCorrelationMetadata() {
        Envelope<String> command = Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "order.saga.replies", "ask");

        Envelope<Integer> reply = command.withData(42);

        assertThat(reply.sagaId()).isEqualTo(command.sagaId());
        assertThat(reply.correlationId()).isEqualTo(command.correlationId());
        assertThat(reply.replyTo()).isEqualTo("order.saga.replies");
        assertThat(reply.data()).isEqualTo(42);
    }

    @Test
    void invalidJsonIsNotRetryable() {
        assertThatThrownBy(() -> codec.readEnvelope("{not json", Ping.class))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("Envelope<Ping>");
    }

    @Test
    void readsPlainObjects() {
        assertThat(codec.read("{\"name\":\"x\",\"count\":1}", Ping.class)).isEqualTo(new Ping("x", 1));
    }
}
```


Create `common/src/test/java/com/shop/common/messaging/InboundMessageTest.java`:

```java
package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class InboundMessageTest {

    private static ConsumerRecord<String, String> record(String id, String type, String value) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("product.commands", 2, 17L, "order-1", value);
        if (id != null) {
            record.headers().add("id", id.getBytes(StandardCharsets.UTF_8));
        }
        if (type != null) {
            record.headers().add("type", type.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Test
    void readsIdAndTypeFromHeaders() {
        UUID id = UUID.randomUUID();

        InboundMessage message = InboundMessage.from(record(id.toString(), "ValidateProducts", "{}"));

        assertThat(message.eventId()).isEqualTo(id);
        assertThat(message.type()).isEqualTo("ValidateProducts");
        assertThat(message.key()).isEqualTo("order-1");
        assertThat(message.payload()).isEqualTo("{}");
        assertThat(message.topic()).isEqualTo("product.commands");
        assertThat(message.partition()).isEqualTo(2);
        assertThat(message.offset()).isEqualTo(17L);
    }

    @Test
    void missingIdHeaderIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record(null, "ValidateProducts", "{}")))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("'id'");
    }

    @Test
    void missingTypeHeaderIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record(UUID.randomUUID().toString(), null, "{}")))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("'type'");
    }

    @Test
    void idThatIsNotAUuidIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record("42", "ValidateProducts", "{}")))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("not a UUID");
    }

    @Test
    void emptyPayloadIsNotRetryable() {
        assertThatThrownBy(() -> InboundMessage.from(record(UUID.randomUUID().toString(), "ValidateProducts", null)))
                .isInstanceOf(NonRetryableException.class);
    }
}
```


Create `common/src/test/java/com/shop/common/messaging/HandlerRegistryTest.java`:

```java
package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class HandlerRegistryTest {

    private static final Handler NOOP = message -> Mono.empty();

    @Test
    void findsARegisteredHandler() {
        HandlerRegistry registry = HandlerRegistry.builder().on("ValidateProducts", NOOP).build();

        assertThat(registry.find("ValidateProducts")).containsSame(NOOP);
    }

    @Test
    void unknownTypeIsEmpty() {
        HandlerRegistry registry = HandlerRegistry.builder().on("ValidateProducts", NOOP).build();

        assertThat(registry.find("Other")).isEmpty();
    }

    @Test
    void registeringTheSameTypeTwiceFails() {
        HandlerRegistry.Builder builder = HandlerRegistry.builder().on("ValidateProducts", NOOP);

        assertThatThrownBy(() -> builder.on("ValidateProducts", NOOP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ValidateProducts");
    }
}
```


- [ ] **Step 2: Run them and see them fail**

```bash
cd common && mvn -B -q test -Dtest='JsonCodecTest,InboundMessageTest,HandlerRegistryTest'
```

Expected: `COMPILATION ERROR` (`Envelope`, `JsonCodec`, `NonRetryableException`, ... not found).

- [ ] **Step 3: Implement**

Create `common/src/main/java/com/shop/common/messaging/NonRetryableException.java`:

```java
package com.shop.common.messaging;

/**
 * Thrown for failures that retrying cannot fix (unreadable payload, unknown message type, missing
 * header). The Kafka error handler sends the record straight to the dead letter topic.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
```


Create `common/src/main/java/com/shop/common/messaging/Envelope.java`:

```java
package com.shop.common.messaging;

import java.util.UUID;

/**
 * JSON body of every saga message: correlation metadata plus the business {@code data}.
 * The message type travels separately (outbox column {@code type}, Kafka header {@code type}).
 */
public record Envelope<T>(UUID sagaId, UUID correlationId, String replyTo, T data) {

    public static <T> Envelope<T> of(UUID sagaId, UUID correlationId, String replyTo, T data) {
        return new Envelope<>(sagaId, correlationId, replyTo, data);
    }

    /** Same correlation metadata, different business data (typical for a reply). */
    public <R> Envelope<R> withData(R newData) {
        return new Envelope<>(sagaId, correlationId, replyTo, newData);
    }
}
```


Create `common/src/main/java/com/shop/common/messaging/JsonCodec.java`:

```java
package com.shop.common.messaging;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

/** Thin JSON facade (Jackson 3). Parsing failures are not retryable. */
public class JsonCodec {

    private final JsonMapper mapper;

    public JsonCodec(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JacksonException e) {
            throw new NonRetryableException("Cannot serialise " + value.getClass().getName(), e);
        }
    }

    public <T> T read(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JacksonException e) {
            throw new NonRetryableException("Cannot parse " + type.getSimpleName() + ": " + e.getOriginalMessage(), e);
        }
    }

    public <T> Envelope<T> readEnvelope(String json, Class<T> dataType) {
        try {
            JavaType type = mapper.getTypeFactory().constructParametricType(Envelope.class, dataType);
            return mapper.readValue(json, type);
        } catch (JacksonException e) {
            throw new NonRetryableException(
                    "Cannot parse Envelope<" + dataType.getSimpleName() + ">: " + e.getOriginalMessage(), e);
        }
    }
}
```


Create `common/src/main/java/com/shop/common/messaging/InboundMessage.java`:

```java
package com.shop.common.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** A Kafka record reduced to what handlers need. Built from the headers set by Debezium. */
public record InboundMessage(
        UUID eventId,
        String type,
        String key,
        String payload,
        String topic,
        int partition,
        long offset) {

    public static final String HEADER_ID = "id";
    public static final String HEADER_TYPE = "type";

    public static InboundMessage from(ConsumerRecord<String, String> record) {
        String id = header(record, HEADER_ID)
                .orElseThrow(() -> new NonRetryableException("Missing Kafka header '" + HEADER_ID + "'"));
        String type = header(record, HEADER_TYPE)
                .orElseThrow(() -> new NonRetryableException("Missing Kafka header '" + HEADER_TYPE + "'"));
        if (record.value() == null) {
            throw new NonRetryableException("Empty payload for message type " + type);
        }
        return new InboundMessage(
                parseUuid(id), type, record.key(), record.value(), record.topic(), record.partition(), record.offset());
    }

    public <T> Envelope<T> envelope(JsonCodec codec, Class<T> dataType) {
        return codec.readEnvelope(payload, dataType);
    }

    private static Optional<String> header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return Optional.empty();
        }
        return Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }

    private static UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new NonRetryableException("Header '" + HEADER_ID + "' is not a UUID: " + value, e);
        }
    }
}
```


Create `common/src/main/java/com/shop/common/messaging/Handler.java`:

```java
package com.shop.common.messaging;

import java.util.function.Function;

import reactor.core.publisher.Mono;

/**
 * Business reaction to one message type. Everything it does (database writes, outbox rows)
 * happens inside the {@code Mono}, which the dispatcher runs in a single transaction.
 */
@FunctionalInterface
public interface Handler extends Function<InboundMessage, Mono<Void>> {
}
```


Create `common/src/main/java/com/shop/common/messaging/HandlerRegistry.java`:

```java
package com.shop.common.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Immutable lookup from message {@code type} to {@link Handler}. */
public final class HandlerRegistry {

    private final Map<String, Handler> handlers;

    private HandlerRegistry(Map<String, Handler> handlers) {
        this.handlers = Map.copyOf(handlers);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<Handler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public static final class Builder {

        private final Map<String, Handler> handlers = new HashMap<>();

        public Builder on(String type, Handler handler) {
            if (handlers.putIfAbsent(type, handler) != null) {
                throw new IllegalStateException("Handler already registered for type " + type);
            }
            return this;
        }

        public HandlerRegistry build() {
            return new HandlerRegistry(handlers);
        }
    }
}
```


- [ ] **Step 4: Run them and see them pass**

```bash
cd common && mvn -B test -Dtest='JsonCodecTest,InboundMessageTest,HandlerRegistryTest'
```

Expected: `Tests run: 12, Failures: 0, Errors: 0` (4 + 5 + 3).

- [ ] **Step 5: Commit**

```bash
git add common
git commit -m "feat(common): envelope, JSON codec, inbound message and handler registry"
```

### Task 6: Inbox, outbox writer and message dispatcher
**Files:**
- Create: `common/src/main/java/com/shop/common/inbox/Inbox.java`
- Create: `common/src/main/java/com/shop/common/outbox/OutboxWriter.java`
- Create: `common/src/test/java/com/shop/common/messaging/MessageDispatcherTest.java`
- Create: `common/src/main/java/com/shop/common/messaging/MessageDispatcher.java`

- [ ] **Step 1: Write the failing dispatcher test**

Create `common/src/test/java/com/shop/common/messaging/MessageDispatcherTest.java`:

```java
package com.shop.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.shop.common.inbox.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.reactive.TransactionalOperator;

class MessageDispatcherTest {

    private static final String CONSUMER = "product-service.product.commands";

    private final Inbox inbox = mock(Inbox.class);
    private final TransactionalOperator transactions = mock(TransactionalOperator.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final AtomicInteger handled = new AtomicInteger();

    private MessageDispatcher dispatcher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // No real transaction in a unit test: the operator simply returns the Mono it is given.
        when(transactions.transactional(any(Mono.class))).thenAnswer(invocation -> invocation.getArgument(0));
        HandlerRegistry registry = HandlerRegistry.builder()
                .on("ValidateProducts", message -> Mono.fromRunnable(handled::incrementAndGet))
                .on("Boom", message -> Mono.error(new IllegalStateException("database down")))
                .build();
        dispatcher = new MessageDispatcher(CONSUMER, registry, inbox, transactions, Duration.ofSeconds(2));
    }

    private static ConsumerRecord<String, String> record(UUID id, String type) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("product.commands", 0, 1L, "order-1", "{}");
        record.headers().add("id", id.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("type", type.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    /** Inbox that accepts the event and runs the effect, as the real one does for a new event id. */
    @SuppressWarnings("unchecked")
    private void inboxAccepts(UUID id) {
        when(inbox.once(eq(CONSUMER), eq(id), any())).thenAnswer(invocation ->
                ((Supplier<Mono<Void>>) invocation.getArgument(2)).get().thenReturn(true));
    }

    @Test
    void runsTheHandlerThenAcknowledges() {
        UUID id = UUID.randomUUID();
        inboxAccepts(id);

        dispatcher.onMessage(record(id, "ValidateProducts"), acknowledgment);

        assertThat(handled).hasValue(1);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void duplicateEventSkipsTheHandlerButStillAcknowledges() {
        UUID id = UUID.randomUUID();
        when(inbox.once(eq(CONSUMER), eq(id), any())).thenReturn(Mono.just(false));

        dispatcher.onMessage(record(id, "ValidateProducts"), acknowledgment);

        assertThat(handled).hasValue(0);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void handlerFailureIsPropagatedAndNothingIsAcknowledged() {
        UUID id = UUID.randomUUID();
        inboxAccepts(id);

        assertThatThrownBy(() -> dispatcher.onMessage(record(id, "Boom"), acknowledgment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("database down");

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void unknownTypeIsNotRetryableAndTouchesNothing() {
        UUID id = UUID.randomUUID();

        assertThatThrownBy(() -> dispatcher.onMessage(record(id, "Unknown"), acknowledgment))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("Unknown");

        verify(inbox, never()).once(any(), any(), any());
        verify(acknowledgment, never()).acknowledge();
    }
}
```


- [ ] **Step 2: Run it and see it fail**

```bash
cd common && mvn -B -q test -Dtest=MessageDispatcherTest
```

Expected: `COMPILATION ERROR` (`Inbox`, `MessageDispatcher` not found).

- [ ] **Step 3: Implement the inbox (`processed_events`, `ON CONFLICT DO NOTHING`)**

Create `common/src/main/java/com/shop/common/inbox/Inbox.java`:

```java
package com.shop.common.inbox;

import java.util.UUID;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Idempotent-consumer guard backed by the {@code processed_events} table.
 * Run it inside the same transaction as the business effect: either both commit or neither does.
 */
public class Inbox {

    private static final String INSERT = """
            INSERT INTO processed_events (consumer_name, event_id)
            VALUES (:consumerName, :eventId)
            ON CONFLICT DO NOTHING
            """;

    private final DatabaseClient db;

    public Inbox(DatabaseClient db) {
        this.db = db;
    }

    /**
     * Records the event, then runs {@code effect}.
     *
     * @return {@code true} if the effect ran, {@code false} if the event had already been processed
     */
    public Mono<Boolean> once(String consumerName, UUID eventId, Supplier<Mono<Void>> effect) {
        return db.sql(INSERT)
                .bind("consumerName", consumerName)
                .bind("eventId", eventId)
                .fetch()
                .rowsUpdated()
                .flatMap(inserted -> inserted == 0
                        ? Mono.just(false)
                        : Mono.defer(effect).thenReturn(true));
    }
}
```


- [ ] **Step 4: Implement the outbox writer (`outbox_events`, JSONB payload)**

Create `common/src/main/java/com/shop/common/outbox/OutboxWriter.java`:

```java
package com.shop.common.outbox;

import java.util.UUID;

import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.JsonCodec;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Writes a message to the {@code outbox_events} table. Debezium turns the row into a Kafka record.
 * Call it inside the transaction that changes the business state.
 */
public class OutboxWriter {

    private static final String INSERT = """
            INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
            VALUES (:id, :topic, :aggregateType, :aggregateId, :type, CAST(:payload AS jsonb))
            """;

    private final DatabaseClient db;
    private final JsonCodec codec;

    public OutboxWriter(DatabaseClient db, JsonCodec codec) {
        this.db = db;
        this.codec = codec;
    }

    /**
     * @param topic         destination Kafka topic (Debezium routes on this column)
     * @param aggregateType kind of aggregate, e.g. {@code Order}
     * @param aggregateId   Kafka record key; use the order id to keep per-order ordering
     * @param type          message type, becomes the Kafka header {@code type}
     * @return the id of the outbox row, which is also the Kafka header {@code id}
     */
    public <T> Mono<UUID> write(String topic, String aggregateType, String aggregateId, String type,
                                Envelope<T> envelope) {
        UUID id = UUID.randomUUID();
        return Mono.fromSupplier(() -> codec.write(envelope))
                .flatMap(json -> db.sql(INSERT)
                        .bind("id", id)
                        .bind("topic", topic)
                        .bind("aggregateType", aggregateType)
                        .bind("aggregateId", aggregateId)
                        .bind("type", type)
                        .bind("payload", json)
                        .fetch()
                        .rowsUpdated())
                .thenReturn(id);
    }
}
```


- [ ] **Step 5: Implement the dispatcher. The transaction wraps inbox + handler + outbox; the offset is acknowledged only after commit. `block()` is legitimate on a Kafka listener thread**

Create `common/src/main/java/com/shop/common/messaging/MessageDispatcher.java`:

```java
package com.shop.common.messaging;

import java.time.Duration;

import com.shop.common.inbox.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * One per consumed topic. Called from a Spring Kafka listener thread (one thread per partition):
 *
 * <ol>
 *   <li>parse headers, find the {@link Handler} for the message type;</li>
 *   <li>in one transaction: insert into the inbox, then run the handler (business effect + outbox);</li>
 *   <li>acknowledge the offset only after the transaction committed.</li>
 * </ol>
 *
 * <p>{@code block()} is legitimate here: the listener thread is neither a Netty nor a Reactor
 * thread, and Kafka already guarantees a single in-flight record per partition.
 */
public class MessageDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MessageDispatcher.class);

    private final String consumerName;
    private final HandlerRegistry registry;
    private final Inbox inbox;
    private final TransactionalOperator transactions;
    private final Duration timeout;

    /**
     * @param consumerName inbox key, convention {@code <service>.<topic>}
     * @param timeout      upper bound for one message; exceeding it fails the record (retried)
     */
    public MessageDispatcher(String consumerName, HandlerRegistry registry, Inbox inbox,
                             TransactionalOperator transactions, Duration timeout) {
        this.consumerName = consumerName;
        this.registry = registry;
        this.inbox = inbox;
        this.transactions = transactions;
        this.timeout = timeout;
    }

    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        InboundMessage message = InboundMessage.from(record);
        Handler handler = registry.find(message.type())
                .orElseThrow(() -> new NonRetryableException(
                        "No handler for message type '" + message.type() + "' on " + message.topic()));

        Boolean processed = transactions
                .transactional(inbox.once(consumerName, message.eventId(), () -> handler.apply(message)))
                .block(timeout);

        if (Boolean.TRUE.equals(processed)) {
            log.debug("Processed {} id={} key={}", message.type(), message.eventId(), message.key());
        } else {
            log.info("Skipped duplicate {} id={} key={}", message.type(), message.eventId(), message.key());
        }
        acknowledgment.acknowledge();
    }
}
```


- [ ] **Step 6: Run the test and see it pass**

```bash
cd common && mvn -B test -Dtest=MessageDispatcherTest
```

Expected: `Tests run: 4, Failures: 0, Errors: 0`.

- [ ] **Step 7: Commit**

```bash
git add common
git commit -m "feat(common): inbox, outbox writer and transactional message dispatcher"
```

### Task 7: Retention purge
**Files:**
- Create: `common/src/main/java/com/shop/common/inbox/RetentionPurger.java`
- Create: `common/src/test/java/com/shop/common/config/RetentionJobTest.java`
- Create: `common/src/main/java/com/shop/common/config/{RetentionProperties,RetentionConfig}.java`

- [ ] **Step 1: Implement the batch purger**

Create `common/src/main/java/com/shop/common/inbox/RetentionPurger.java`:

```java
package com.shop.common.inbox;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;

/** Deletes old inbox and outbox rows in small batches so the tables stay lean without long locks. */
public class RetentionPurger {

    private static final Logger log = LoggerFactory.getLogger(RetentionPurger.class);

    private static final String PURGE_INBOX = """
            DELETE FROM processed_events
            WHERE ctid IN (SELECT ctid FROM processed_events WHERE processed_at < :cutoff LIMIT :batch)
            """;
    private static final String PURGE_OUTBOX = """
            DELETE FROM outbox_events
            WHERE ctid IN (SELECT ctid FROM outbox_events WHERE occurred_at < :cutoff LIMIT :batch)
            """;

    private final DatabaseClient db;
    private final Duration inboxRetention;
    private final Duration outboxRetention;
    private final int batchSize;

    public RetentionPurger(DatabaseClient db, Duration inboxRetention, Duration outboxRetention, int batchSize) {
        this.db = db;
        this.inboxRetention = inboxRetention;
        this.outboxRetention = outboxRetention;
        this.batchSize = batchSize;
    }

    /** Purges both tables; returns the total number of deleted rows. */
    public Mono<Long> purge() {
        return purgeInBatches(PURGE_INBOX, inboxRetention)
                .zipWith(purgeInBatches(PURGE_OUTBOX, outboxRetention), Long::sum)
                .doOnNext(total -> {
                    if (total > 0) {
                        log.info("Retention purge deleted {} rows", total);
                    }
                });
    }

    private Mono<Long> purgeInBatches(String sql, Duration retention) {
        OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(retention);
        Mono<Long> oneBatch = Mono.defer(() -> db.sql(sql)
                .bind("cutoff", cutoff)
                .bind("batch", batchSize)
                .fetch()
                .rowsUpdated());
        return oneBatch
                .expand(deleted -> deleted >= batchSize ? oneBatch : Mono.empty())
                .reduce(0L, Long::sum);
    }
}
```


- [ ] **Step 2: Write the failing test for the scheduled job. Spring calls a reactive `@Scheduled` method once and re-subscribes to the returned `Mono` on every run, while `purge()` computes its time cutoff when called: the job must defer the call, or the cutoff stays frozen at the application's start time**

Create `common/src/test/java/com/shop/common/config/RetentionJobTest.java`:

```java
package com.shop.common.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.shop.common.config.RetentionConfig.PurgeJob;
import com.shop.common.inbox.RetentionPurger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class RetentionJobTest {

    /**
     * Spring invokes a reactive {@code @Scheduled} method once and re-subscribes to the returned Mono on
     * every run. The purge cutoff is computed when {@code purge()} is called, so the job must call it again
     * at each subscription, otherwise the cutoff is frozen at the time the application started.
     */
    @Test
    void callsThePurgerAgainOnEverySubscription() {
        RetentionPurger purger = mock(RetentionPurger.class);
        when(purger.purge()).thenReturn(Mono.just(0L));
        PurgeJob job = new PurgeJob(purger);

        Mono<Void> scheduled = job.run();
        scheduled.block();
        scheduled.block();
        scheduled.block();

        verify(purger, times(3)).purge();
    }
}
```


- [ ] **Step 3: Run it and see it fail**

```bash
cd common && mvn -B -q test -Dtest=RetentionJobTest
```

Expected: `COMPILATION ERROR`, `cannot find symbol ... RetentionConfig`.

- [ ] **Step 4: Implement the properties and the scheduled job (`Mono.defer(purger::purge)`)**

Create `common/src/main/java/com/shop/common/config/RetentionProperties.java`:

```java
package com.shop.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Housekeeping for {@code processed_events} and {@code outbox_events}.
 * Keep {@code inbox} longer than the Kafka topic retention plus the longest expected redelivery window.
 */
@ConfigurationProperties(prefix = "shop.retention")
public record RetentionProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("7d") Duration inbox,
        @DefaultValue("3d") Duration outbox,
        @DefaultValue("1000") int batchSize,
        @DefaultValue("5m") Duration interval) {
}
```


Create `common/src/main/java/com/shop/common/config/RetentionConfig.java`:

```java
package com.shop.common.config;

import com.shop.common.inbox.RetentionPurger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(RetentionProperties.class)
@ConditionalOnProperty(prefix = "shop.retention", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RetentionConfig {

    private static final Logger log = LoggerFactory.getLogger(RetentionConfig.class);

    @Bean
    public RetentionPurger retentionPurger(DatabaseClient databaseClient, RetentionProperties props) {
        return new RetentionPurger(databaseClient, props.inbox(), props.outbox(), props.batchSize());
    }

    @Bean
    public PurgeJob purgeJob(RetentionPurger purger) {
        return new PurgeJob(purger);
    }

    /** Reactive {@code @Scheduled} method: Spring subscribes to the returned {@code Mono}. */
    public static class PurgeJob {

        private final RetentionPurger purger;

        PurgeJob(RetentionPurger purger) {
            this.purger = purger;
        }

        @Scheduled(fixedDelayString = "${shop.retention.interval:5m}", initialDelayString = "${shop.retention.interval:5m}")
        public Mono<Void> run() {
            // Spring calls this method once and re-subscribes to the Mono on every run, so anything that
            // depends on the current time (the purge cutoff) must be computed at subscription: defer.
            return Mono.defer(purger::purge)
                    .doOnError(e -> log.warn("Retention purge failed, will retry next run", e))
                    .onErrorResume(e -> Mono.empty())
                    .then();
        }
    }
}
```


- [ ] **Step 5: Run the test and see it pass**

```bash
cd common && mvn -B test -Dtest=RetentionJobTest
```

Expected: `Tests run: 1, Failures: 0, Errors: 0`. Without `Mono.defer` it fails with `Wanted 3 times ... but was 1`.

- [ ] **Step 6: Commit**

```bash
git add common
git commit -m "feat(common): scheduled batch purge of inbox and outbox tables"
```

### Task 8: Kafka consumer configuration
**Files:**
- Create: `common/src/main/java/com/shop/common/config/{KafkaConsumerProperties,KafkaConsumerConfig}.java`

- [ ] **Step 1: Implement. Listener container factory `shopKafkaListenerContainerFactory`: manual ack, `read_committed`, no auto-commit, exponential retry, dead letter to `<topic>.DLT` on the same partition, `NonRetryableException` goes straight to the dead letter topic, and `commitRecovered` so a dead-lettered record is not redelivered. Retries run on the partition's consumer thread, so keep the retry total well below `max-poll-interval`**

Create `common/src/main/java/com/shop/common/config/KafkaConsumerProperties.java`:

```java
package com.shop.common.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Kafka consumer settings ({@code shop.kafka.*}). The consumer configuration is only active when
 * {@code shop.kafka.bootstrap-servers} is set.
 */
@ConfigurationProperties(prefix = "shop.kafka")
public record KafkaConsumerProperties(
        String bootstrapServers,
        String groupId,
        @DefaultValue("3") int concurrency,
        @DefaultValue("100") int maxPollRecords,
        @DefaultValue("5m") Duration maxPollInterval,
        @DefaultValue("45s") Duration sessionTimeout,
        @DefaultValue Retry retry) {

    /**
     * Retry before dead-lettering. Retries block the partition's consumer thread, so keep the
     * total well below {@code maxPollInterval}.
     */
    public record Retry(
            @DefaultValue("500ms") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("10s") Duration maxInterval,
            @DefaultValue("5") int maxAttempts) {
    }
}
```


Create `common/src/main/java/com/shop/common/config/KafkaConsumerConfig.java`:

```java
package com.shop.common.config;

import java.util.HashMap;
import java.util.Map;

import com.shop.common.messaging.NonRetryableException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Listener container factory used by every service consumer: manual acknowledgement (after the
 * database commit), read_committed isolation, exponential retry, then {@code <topic>.DLT}.
 *
 * <p>Services declare {@code @KafkaListener(containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY, ...)}.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
@EnableConfigurationProperties(KafkaConsumerProperties.class)
@ConditionalOnProperty(prefix = "shop.kafka", name = "bootstrap-servers")
public class KafkaConsumerConfig {

    public static final String CONTAINER_FACTORY = "shopKafkaListenerContainerFactory";
    public static final String DLT_SUFFIX = ".DLT";

    @Bean
    public ConsumerFactory<String, String> shopConsumerFactory(KafkaConsumerProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.maxPollRecords());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) props.maxPollInterval().toMillis());
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) props.sessionTimeout().toMillis());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /** Producer used only to publish dead letters. */
    @Bean
    public ProducerFactory<String, String> shopDltProducerFactory(KafkaConsumerProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> shopDltKafkaTemplate(ProducerFactory<String, String> shopDltProducerFactory) {
        return new KafkaTemplate<>(shopDltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler shopKafkaErrorHandler(KafkaTemplate<String, String> shopDltKafkaTemplate,
                                                     KafkaConsumerProperties props) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                shopDltKafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));

        KafkaConsumerProperties.Retry retry = props.retry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxAttempts());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableException.class);
        // Manual ack mode: commit the offset of a record that was dead-lettered.
        handler.setCommitRecovered(true);
        return handler;
    }

    @Bean(name = CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, String> shopKafkaListenerContainerFactory(
            ConsumerFactory<String, String> shopConsumerFactory,
            DefaultErrorHandler shopKafkaErrorHandler,
            KafkaConsumerProperties props) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(shopConsumerFactory);
        factory.setConcurrency(props.concurrency());
        factory.setCommonErrorHandler(shopKafkaErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
```


- [ ] **Step 2: Compile**

```bash
cd common && mvn -B -q compile
```

Expected: no output, exit code 0. Verified end to end by `MessageDispatcherKafkaIT` in Task 10.

- [ ] **Step 3: Commit**

```bash
git add common
git commit -m "feat(common): Spring Kafka listener container with retry and dead letter topic"
```

### Task 9: Messaging beans and auto-configuration
**Files:**
- Create: `common/src/main/java/com/shop/common/config/{MessagingConfig,CommonAutoConfiguration}.java`
- Create: `common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 1: Implement the messaging beans (`JsonCodec` reuses the application's `JsonMapper` when present)**

Create `common/src/main/java/com/shop/common/config/MessagingConfig.java`:

```java
package com.shop.common.config;

import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.JsonCodec;
import com.shop.common.outbox.OutboxWriter;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;

@Configuration(proxyBeanMethods = false)
public class MessagingConfig {

    /** Reuses the application's {@link JsonMapper} when there is one, otherwise a default mapper. */
    @Bean
    public JsonCodec jsonCodec(ObjectProvider<JsonMapper> mapper) {
        return new JsonCodec(mapper.getIfAvailable(() -> JsonMapper.builder().build()));
    }

    @Bean
    public OutboxWriter outboxWriter(DatabaseClient databaseClient, JsonCodec jsonCodec) {
        return new OutboxWriter(databaseClient, jsonCodec);
    }

    @Bean
    public Inbox inbox(DatabaseClient databaseClient) {
        return new Inbox(databaseClient);
    }
}
```


- [ ] **Step 2: Implement the auto-configuration. It runs before Boot's R2DBC auto-configuration so our pool is the single `ConnectionFactory`**

Create `common/src/main/java/com/shop/common/config/CommonAutoConfiguration.java`:

```java
package com.shop.common.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * Entry point of the library. A service only adds the dependency and sets {@code shop.*}
 * properties; it writes no infrastructure configuration.
 */
@AutoConfiguration(beforeName = "org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration")
@Import({
        FlywayConfig.class,
        R2dbcConnectionConfig.class,
        TransactionConfig.class,
        MessagingConfig.class,
        KafkaConsumerConfig.class,
        RetentionConfig.class
})
public class CommonAutoConfiguration {
}
```


Create `common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:

```text
com.shop.common.config.CommonAutoConfiguration
```


- [ ] **Step 3: Run every unit test**

```bash
cd common && mvn -B test
```

Expected: `Tests run: 24, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add common
git commit -m "feat(common): register shop-common as a Spring Boot auto-configuration"
```

### Task 10: Integration tests with Testcontainers
**Files:**
- Create: `common/src/test/java/com/shop/common/support/{PostgresSupport,KafkaSupport,KafkaTestClient}.java`
- Create: `common/src/test/java/com/shop/common/PostgresInfrastructureIT.java`
- Create: `common/src/test/java/com/shop/common/MessageDispatcherKafkaIT.java`

- [ ] **Step 1: Create the container support. One PostgreSQL 18 and one Kafka container per JVM**

Create `common/src/test/java/com/shop/common/support/PostgresSupport.java`:

```java
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
```


Create `common/src/test/java/com/shop/common/support/KafkaSupport.java`:

```java
package com.shop.common.support;

import org.testcontainers.kafka.KafkaContainer;

/** One Kafka broker (KRaft) shared by every integration test of the JVM. */
public final class KafkaSupport {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static {
        KAFKA.start();
    }

    private KafkaSupport() {
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
```


Create `common/src/test/java/com/shop/common/support/KafkaTestClient.java`:

```java
package com.shop.common.support;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Minimal Kafka client for tests: create topics, publish like Debezium does, read a topic. */
public final class KafkaTestClient {

    private final String bootstrapServers;

    public KafkaTestClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void createTopics(String... topics) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers))) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }

    /** Publishes a record with the {@code id} and {@code type} headers that Debezium's EventRouter sets. */
    public void send(String topic, String key, UUID id, String type, String payload) throws Exception {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
            record.headers().add("id", id.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add("type", type.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }
    }

    /** Reads {@code topic} from the beginning until {@code expected} records arrived or the timeout elapsed. */
    public List<ConsumerRecord<String, String>> read(String topic, int expected, Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            while (records.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
        }
        return records;
    }
}
```


- [ ] **Step 2: Create the PostgreSQL integration test (migration before pool, outbox commit and rollback, inbox idempotence and rollback, batch purge)**

Create `common/src/test/java/com/shop/common/PostgresInfrastructureIT.java`:

```java
package com.shop.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.shop.common.config.CommonAutoConfiguration;
import com.shop.common.config.FlywayConfig.FlywayMigration;
import com.shop.common.inbox.Inbox;
import com.shop.common.inbox.RetentionPurger;
import com.shop.common.messaging.Envelope;
import com.shop.common.outbox.OutboxWriter;
import com.shop.common.support.PostgresSupport;
import io.r2dbc.pool.ConnectionPool;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

/** Boots the library against a real PostgreSQL: migration, pool, transactions, outbox, inbox, purge. */
class PostgresInfrastructureIT {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CommonAutoConfiguration.class))
            .withPropertyValues(PostgresSupport.r2dbcProperties())
            .withPropertyValues("shop.retention.enabled=false");

    private static long count(DatabaseClient db, String sql) {
        return db.sql(sql).map(row -> row.get(0, Long.class)).one().block(Duration.ofSeconds(10));
    }

    @Test
    void migratesTheSchemaBeforeThePoolOpens() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(ConnectionPool.class);
            assertThat(context.getBean(FlywayMigration.class).executed()).isTrue();

            long tables = count(context.getBean(DatabaseClient.class), """
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_name IN ('processed_events', 'outbox_events')
                    """);
            assertThat(tables).isEqualTo(2);
        });
    }

    @Test
    void outboxRowIsCommittedWithTheBusinessTransaction() {
        runner.run(context -> {
            OutboxWriter outbox = context.getBean(OutboxWriter.class);
            TransactionalOperator tx = context.getBean(TransactionalOperator.class);
            DatabaseClient db = context.getBean(DatabaseClient.class);
            String orderId = "order-" + UUID.randomUUID();
            UUID sagaId = UUID.randomUUID();

            StepVerifier.create(tx.transactional(outbox.write(
                            "product.commands", "Order", orderId, "ValidateProducts",
                            Envelope.of(sagaId, sagaId, "order.saga.replies", "payload"))))
                    .expectNextCount(1)
                    .verifyComplete();

            String row = db.sql("""
                            SELECT topic || '|' || type || '|' || (payload->>'sagaId') || '|' || (payload->>'replyTo')
                            FROM outbox_events WHERE aggregateid = :id
                            """)
                    .bind("id", orderId)
                    .map(r -> r.get(0, String.class))
                    .one().block(Duration.ofSeconds(10));
            assertThat(row).isEqualTo("product.commands|ValidateProducts|" + sagaId + "|order.saga.replies");
        });
    }

    @Test
    void outboxRowIsRolledBackWhenTheTransactionFails() {
        runner.run(context -> {
            OutboxWriter outbox = context.getBean(OutboxWriter.class);
            TransactionalOperator tx = context.getBean(TransactionalOperator.class);
            DatabaseClient db = context.getBean(DatabaseClient.class);
            String orderId = "order-" + UUID.randomUUID();

            StepVerifier.create(tx.transactional(outbox
                            .write("product.commands", "Order", orderId, "ValidateProducts",
                                    Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "r", "x"))
                            .then(Mono.error(new IllegalStateException("business failure")))))
                    .expectError(IllegalStateException.class)
                    .verify();

            assertThat(count(db, "SELECT count(*) FROM outbox_events WHERE aggregateid = '" + orderId + "'"))
                    .isZero();
        });
    }

    @Test
    void inboxRunsTheEffectOnlyOncePerEvent() {
        runner.run(context -> {
            Inbox inbox = context.getBean(Inbox.class);
            AtomicInteger effects = new AtomicInteger();
            UUID eventId = UUID.randomUUID();

            StepVerifier.create(inbox.once("it.consumer", eventId, () -> Mono.fromRunnable(effects::incrementAndGet)))
                    .expectNext(true).verifyComplete();
            StepVerifier.create(inbox.once("it.consumer", eventId, () -> Mono.fromRunnable(effects::incrementAndGet)))
                    .expectNext(false).verifyComplete();

            assertThat(effects).hasValue(1);
        });
    }

    @Test
    void sameEventIdIsIndependentPerConsumer() {
        runner.run(context -> {
            Inbox inbox = context.getBean(Inbox.class);
            UUID eventId = UUID.randomUUID();

            StepVerifier.create(inbox.once("it.consumer-a", eventId, Mono::empty)).expectNext(true).verifyComplete();
            StepVerifier.create(inbox.once("it.consumer-b", eventId, Mono::empty)).expectNext(true).verifyComplete();
        });
    }

    @Test
    void inboxMarkIsRolledBackWhenTheEffectFails() {
        runner.run(context -> {
            Inbox inbox = context.getBean(Inbox.class);
            TransactionalOperator tx = context.getBean(TransactionalOperator.class);
            UUID eventId = UUID.randomUUID();

            StepVerifier.create(tx.transactional(
                            inbox.once("it.consumer", eventId, () -> Mono.error(new IllegalStateException("boom")))))
                    .expectError(IllegalStateException.class)
                    .verify();

            // The failed attempt left no trace, so the redelivery is processed normally.
            StepVerifier.create(tx.transactional(inbox.once("it.consumer", eventId, Mono::empty)))
                    .expectNext(true)
                    .verifyComplete();
        });
    }

    @Test
    void purgesOldRowsInBatchesAndKeepsRecentOnes() {
        runner.run(context -> {
            DatabaseClient db = context.getBean(DatabaseClient.class);
            db.sql("TRUNCATE processed_events, outbox_events").fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    INSERT INTO processed_events (consumer_name, event_id, processed_at)
                    SELECT 'purge', gen_random_uuid(), now() - interval '10 days' FROM generate_series(1, 25)
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    INSERT INTO processed_events (consumer_name, event_id)
                    SELECT 'purge', gen_random_uuid() FROM generate_series(1, 5)
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload, occurred_at)
                    SELECT gen_random_uuid(), 't', 'A', 'k', 'T', '{}'::jsonb, now() - interval '5 days'
                    FROM generate_series(1, 12)
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));

            RetentionPurger purger = new RetentionPurger(db, Duration.ofDays(7), Duration.ofDays(3), 10);

            StepVerifier.create(purger.purge()).expectNext(37L).verifyComplete();
            assertThat(count(db, "SELECT count(*) FROM processed_events")).isEqualTo(5);
            assertThat(count(db, "SELECT count(*) FROM outbox_events")).isZero();
        });
    }
}
```


- [ ] **Step 3: Create the Kafka integration test (dispatch, duplicate delivery, retry then dead letter, non-retryable, progress after a dead letter)**

Create `common/src/test/java/com/shop/common/MessageDispatcherKafkaIT.java`:

```java
package com.shop.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.shop.common.config.CommonAutoConfiguration;
import com.shop.common.config.KafkaConsumerConfig;
import com.shop.common.inbox.Inbox;
import com.shop.common.messaging.Envelope;
import com.shop.common.messaging.HandlerRegistry;
import com.shop.common.messaging.MessageDispatcher;
import com.shop.common.messaging.NonRetryableException;
import com.shop.common.outbox.OutboxWriter;
import com.shop.common.support.KafkaSupport;
import com.shop.common.support.KafkaTestClient;
import com.shop.common.support.PostgresSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Real PostgreSQL + real Kafka: dispatch, idempotence, same-transaction outbox, retry, dead letter,
 * and progress after a dead-lettered record.
 */
class MessageDispatcherKafkaIT {

    private static final String CONSUMER = "it-service.it.commands";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CommonAutoConfiguration.class))
            .withPropertyValues(PostgresSupport.r2dbcProperties())
            .withPropertyValues(
                    "shop.retention.enabled=false",
                    "shop.kafka.bootstrap-servers=" + KafkaSupport.bootstrapServers(),
                    "shop.kafka.group-id=it-" + UUID.randomUUID(),
                    "shop.kafka.concurrency=1",
                    "shop.kafka.retry.initial-interval=50ms",
                    "shop.kafka.retry.max-interval=100ms",
                    "shop.kafka.retry.max-attempts=2");

    private static String payload() {
        return "{\"sagaId\":\"%s\",\"correlationId\":\"%s\",\"replyTo\":\"order.saga.replies\",\"data\":\"x\"}"
                .formatted(UUID.randomUUID(), UUID.randomUUID());
    }

    @Test
    void dispatchesDeduplicatesRetriesAndDeadLetters() {
        String topic = "it.commands." + UUID.randomUUID();
        String dlt = topic + KafkaConsumerConfig.DLT_SUFFIX;
        AtomicInteger counted = new AtomicInteger();
        AtomicInteger failing = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();

        runner.run(context -> {
            KafkaTestClient kafka = new KafkaTestClient(KafkaSupport.bootstrapServers());
            kafka.createTopics(topic, dlt);

            DatabaseClient db = context.getBean(DatabaseClient.class);
            OutboxWriter outbox = context.getBean(OutboxWriter.class);
            db.sql("TRUNCATE processed_events, outbox_events").fetch().rowsUpdated().block(Duration.ofSeconds(10));

            HandlerRegistry registry = HandlerRegistry.builder()
                    .on("Count", message -> {
                        counted.incrementAndGet();
                        return outbox.write("it.events", "Test", message.key(), "Counted",
                                Envelope.of(UUID.randomUUID(), UUID.randomUUID(), "r", "done")).then();
                    })
                    .on("Fail", message -> {
                        failing.incrementAndGet();
                        return Mono.error(new IllegalStateException("database down"));
                    })
                    .on("Bad", message -> {
                        bad.incrementAndGet();
                        return Mono.error(new NonRetryableException("payload cannot be understood"));
                    })
                    .build();
            MessageDispatcher dispatcher = new MessageDispatcher(CONSUMER, registry,
                    context.getBean(Inbox.class), context.getBean(TransactionalOperator.class), Duration.ofSeconds(10));

            @SuppressWarnings("unchecked")
            ConcurrentKafkaListenerContainerFactory<String, String> factory = context.getBean(
                    KafkaConsumerConfig.CONTAINER_FACTORY, ConcurrentKafkaListenerContainerFactory.class);
            ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topic);
            container.getContainerProperties().setMessageListener(
                    (AcknowledgingMessageListener<String, String>) dispatcher::onMessage);
            container.start();
            try {
                // 1. the same event delivered twice is handled once, and its outbox row is written once
                UUID duplicated = UUID.randomUUID();
                kafka.send(topic, "order-1", duplicated, "Count", payload());
                kafka.send(topic, "order-1", duplicated, "Count", payload());
                kafka.send(topic, "order-2", UUID.randomUUID(), "Count", payload());
                await().atMost(Duration.ofSeconds(30)).untilAtomic(counted, org.hamcrest.Matchers.equalTo(2));
                assertThat(db.sql("SELECT count(*) FROM outbox_events").map(r -> r.get(0, Long.class)).one()
                        .block(Duration.ofSeconds(10))).isEqualTo(2);

                // 2. a transient failure is retried (1 delivery + 2 retries) and then dead-lettered
                UUID failingId = UUID.randomUUID();
                String failingPayload = payload();
                kafka.send(topic, "order-3", failingId, "Fail", failingPayload);
                List<ConsumerRecord<String, String>> deadLetters = kafka.read(dlt, 1, Duration.ofSeconds(30));
                assertThat(deadLetters).hasSize(1);
                assertThat(failing).hasValue(3);
                ConsumerRecord<String, String> deadLetter = deadLetters.get(0);
                assertThat(deadLetter.key()).isEqualTo("order-3");
                assertThat(deadLetter.value()).isEqualTo(failingPayload);
                assertThat(new String(deadLetter.headers().lastHeader("id").value(), StandardCharsets.UTF_8))
                        .isEqualTo(failingId.toString());

                // 3. a non-retryable failure is dead-lettered without any retry
                kafka.send(topic, "order-4", UUID.randomUUID(), "Bad", payload());
                assertThat(kafka.read(dlt, 2, Duration.ofSeconds(30))).hasSize(2);
                assertThat(bad).hasValue(1);

                // 4. the consumer keeps going after dead letters
                kafka.send(topic, "order-5", UUID.randomUUID(), "Count", payload());
                await().atMost(Duration.ofSeconds(30)).untilAtomic(counted, org.hamcrest.Matchers.equalTo(3));
            } finally {
                container.stop();
            }
        });
    }
}
```


- [ ] **Step 4: Run everything. Docker must be running; the first run pulls `postgres:18-alpine` and `apache/kafka:4.1.0`**

```bash
cd common && mvn -B verify
```

Expected: 24 unit tests, then `Tests run: 7 ... PostgresInfrastructureIT`, `Tests run: 1 ... MessageDispatcherKafkaIT`, `Tests run: 8, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

If you see `Could not find a valid Docker environment`, start Docker and rerun. If `MessageDispatcherKafkaIT` fails on `apache/kafka:4.1.0`, check the tag exists on Docker Hub and use the closest 4.x tag (keep it equal in `docker-compose.yml`).

- [ ] **Step 5: Commit**

```bash
git add common
git commit -m "test(common): Testcontainers integration tests for outbox, inbox, purge and Kafka consumer"
```

### Task 11: Publish to the local Maven repository
**Files:**
- Modify: none (the POM already has `maven-source-plugin`, `maven-javadoc-plugin` and a placeholder `distributionManagement`)

- [ ] **Step 1: Install**

```bash
cd common && mvn -B install -DskipTests -DskipITs
```

Expected: `Building jar: .../shop-common-1.0.0-SNAPSHOT.jar`, `-sources.jar`, `-javadoc.jar`, then three or four `Installing ...` lines and `BUILD SUCCESS`.

- [ ] **Step 2: Check the installed artifacts**

```bash
ls ~/.m2/repository/com/shop/shop-common/1.0.0-SNAPSHOT/
```

Expected: the `.pom`, `.jar`, `-sources.jar` and `-javadoc.jar` files.

- [ ] **Step 3: Know how to switch to Artifactory later (no change today)**

When Artifactory exists: add a `<server>` with id `artifactory-releases` / `artifactory-snapshots` and credentials to `~/.m2/settings.xml`, then run:

```bash
cd common && mvn -B deploy -Dartifactory.url=https://<your-host>/artifactory
```

The default `https://artifactory.invalid/artifactory` is a deliberate placeholder that can never receive an upload by accident.

### Task 12: Local infrastructure and outbox smoke test
**Files:**
- Create: `infra/docker-compose.yml`
- Create: `infra/debezium/{product,order,inventory}-outbox.json`, `infra/debezium/smoke/smoke-outbox.json`
- Create: `infra/scripts/register-connectors.sh`, `infra/scripts/smoke-outbox.sh`

- [ ] **Step 1: Create the compose file (three PostgreSQL 18 with logical replication, Kafka in KRaft mode, topic creation, Debezium Connect, optional Kafka UI)**

Create `infra/docker-compose.yml`:

```yaml
name: shop

# Local development infrastructure: one PostgreSQL per service (logical replication on),
# Kafka (KRaft), Debezium Connect, optional Kafka UI.
#
# Host ports: product-db 5441, order-db 5442, inventory-db 5443, Kafka 29092, Connect 8083, Kafka UI 8080.
# Services running on the host connect to Kafka at localhost:29092.

x-postgres: &postgres
  image: postgres:18-alpine
  command: ["postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"]
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U postgres"]
    interval: 5s
    timeout: 3s
    retries: 20

services:
  product-db:
    <<: *postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: product_db
    ports:
      - "5441:5432"
    volumes:
      - product-db-data:/var/lib/postgresql

  order-db:
    <<: *postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: order_db
    ports:
      - "5442:5432"
    volumes:
      - order-db-data:/var/lib/postgresql

  inventory-db:
    <<: *postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: inventory_db
    ports:
      - "5443:5432"
    volumes:
      - inventory-db-data:/var/lib/postgresql

  kafka:
    image: apache/kafka:4.1.0
    hostname: kafka
    ports:
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20

  kafka-init:
    image: apache/kafka:4.1.0
    depends_on:
      kafka:
        condition: service_healthy
    restart: "no"
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        for topic in product.commands inventory.commands order.saga.replies order.events; do
          /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $$topic --partitions 3 --replication-factor 1
          /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $$topic.DLT --partitions 3 --replication-factor 1
        done

  connect:
    image: quay.io/debezium/connect:3.7.0.Final
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: shop-connect
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8083/connectors > /dev/null"]
      interval: 5s
      timeout: 5s
      retries: 30

  # Optional: docker compose --profile ui up -d
  kafka-ui:
    profiles: ["ui"]
    image: ghcr.io/kafbat/kafka-ui:latest
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: debezium
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://connect:8083

volumes:
  product-db-data:
  order-db-data:
  inventory-db-data:
```


- [ ] **Step 2: Create the connector definitions. The outbox column `topic` drives routing, `aggregateid` is the Kafka key, the column `type` becomes the header `type`, the row `id` becomes the header `id`, and the JSON payload is expanded into the record value**

Create `infra/debezium/product-outbox.json`:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "plugin.name": "pgoutput",
  "database.hostname": "product-db",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "product_db",
  "topic.prefix": "product",
  "table.include.list": "public.outbox_events",
  "slot.name": "product_outbox_slot",
  "publication.name": "product_outbox_pub",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "no_data",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "aggregateid",
  "transforms.outbox.table.field.event.timestamp": "occurred_at",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.table.fields.additional.placement": "type:header:type",
  "transforms.outbox.table.expand.json.payload": "true",
  "transforms.outbox.route.by.field": "topic",
  "transforms.outbox.route.topic.replacement": "${routedByValue}"
}
```


Create `infra/debezium/order-outbox.json`:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "plugin.name": "pgoutput",
  "database.hostname": "order-db",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "order_db",
  "topic.prefix": "order",
  "table.include.list": "public.outbox_events",
  "slot.name": "order_outbox_slot",
  "publication.name": "order_outbox_pub",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "no_data",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "aggregateid",
  "transforms.outbox.table.field.event.timestamp": "occurred_at",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.table.fields.additional.placement": "type:header:type",
  "transforms.outbox.table.expand.json.payload": "true",
  "transforms.outbox.route.by.field": "topic",
  "transforms.outbox.route.topic.replacement": "${routedByValue}"
}
```


Create `infra/debezium/inventory-outbox.json`:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "plugin.name": "pgoutput",
  "database.hostname": "inventory-db",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "inventory_db",
  "topic.prefix": "inventory",
  "table.include.list": "public.outbox_events",
  "slot.name": "inventory_outbox_slot",
  "publication.name": "inventory_outbox_pub",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "no_data",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "aggregateid",
  "transforms.outbox.table.field.event.timestamp": "occurred_at",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.table.fields.additional.placement": "type:header:type",
  "transforms.outbox.table.expand.json.payload": "true",
  "transforms.outbox.route.by.field": "topic",
  "transforms.outbox.route.topic.replacement": "${routedByValue}"
}
```


Create `infra/debezium/smoke/smoke-outbox.json`:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "plugin.name": "pgoutput",
  "database.hostname": "product-db",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "smoke_db",
  "topic.prefix": "smoke",
  "table.include.list": "public.outbox_events",
  "slot.name": "smoke_outbox_slot",
  "publication.name": "smoke_outbox_pub",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "no_data",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.id": "id",
  "transforms.outbox.table.field.event.key": "aggregateid",
  "transforms.outbox.table.field.event.timestamp": "occurred_at",
  "transforms.outbox.table.field.event.payload": "payload",
  "transforms.outbox.table.fields.additional.placement": "type:header:type",
  "transforms.outbox.table.expand.json.payload": "true",
  "transforms.outbox.route.by.field": "topic",
  "transforms.outbox.route.topic.replacement": "${routedByValue}"
}
```


- [ ] **Step 3: Create the scripts**

Create `infra/scripts/register-connectors.sh`:

```bash
#!/usr/bin/env bash
# Registers (or updates) the Debezium outbox connectors.
# Run it AFTER each service has started once, so Flyway created its outbox_events table.
#
#   ./scripts/register-connectors.sh                 # all three connectors
#   ./scripts/register-connectors.sh product order   # only the named ones
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")/.." && pwd)/debezium"

names=("$@")
if [ ${#names[@]} -eq 0 ]; then
  names=(product order inventory)
fi

echo "Waiting for Kafka Connect at $CONNECT_URL ..."
for _ in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors" > /dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "$CONNECT_URL/connectors" > /dev/null || { echo "Kafka Connect is not reachable" >&2; exit 1; }

for service in "${names[@]}"; do
  connector="$service-outbox"
  file="$DIR/$connector.json"
  [ -f "$file" ] || { echo "No such connector definition: $file" >&2; exit 1; }
  echo "Registering $connector"
  curl -fsS -X PUT -H 'Content-Type: application/json' --data @"$file" \
    "$CONNECT_URL/connectors/$connector/config" > /dev/null
done

echo
curl -fsS "$CONNECT_URL/connectors?expand=status" | python3 -m json.tool
```


Create `infra/scripts/smoke-outbox.sh`:

```bash
#!/usr/bin/env bash
# End-to-end check of the outbox pipeline with no service involved:
#   row in outbox_events  ->  Debezium EventRouter  ->  Kafka record (key, headers id/type, JSON value)
# Uses a throwaway database (smoke_db) and removes everything afterwards.
# Run from the infra/ directory with the stack up:  docker compose up -d --wait kafka connect product-db
set -euo pipefail

cd "$(dirname "$0")/.."
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
MIGRATION="../common/src/main/resources/db/common/V1__outbox_and_inbox.sql"
EVENT_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SAGA_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
PSQL=(docker compose exec -T product-db psql -v ON_ERROR_STOP=1 -U postgres)

cleanup() {
  curl -fsS -X DELETE "$CONNECT_URL/connectors/smoke-outbox" > /dev/null 2>&1 || true
  sleep 2
  "${PSQL[@]}" -d postgres -c "SELECT pg_drop_replication_slot('smoke_outbox_slot') FROM pg_replication_slots WHERE slot_name = 'smoke_outbox_slot'" > /dev/null 2>&1 || true
  "${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS smoke_db" > /dev/null 2>&1 || true
}
trap cleanup EXIT

echo "1/5 create smoke_db and apply the common migration"
"${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS smoke_db" -c "CREATE DATABASE smoke_db" > /dev/null
"${PSQL[@]}" -d smoke_db < "$MIGRATION" > /dev/null

echo "2/5 register the smoke connector"
curl -fsS -X PUT -H 'Content-Type: application/json' --data @debezium/smoke/smoke-outbox.json \
  "$CONNECT_URL/connectors/smoke-outbox/config" > /dev/null

echo "3/5 wait for the connector to run"
for _ in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors/smoke-outbox/status" 2>/dev/null | grep -q '"state":"RUNNING"'; then
    break
  fi
  sleep 2
done
curl -fsS "$CONNECT_URL/connectors/smoke-outbox/status" | grep -q '"state":"RUNNING"' \
  || { echo "FAIL: connector is not RUNNING"; curl -fsS "$CONNECT_URL/connectors/smoke-outbox/status"; exit 1; }

echo "4/5 insert an outbox row"
"${PSQL[@]}" -d smoke_db > /dev/null <<SQL
INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
VALUES ('$EVENT_ID', 'smoke.commands', 'Order', 'order-42', 'ValidateProducts',
        '{"sagaId":"$SAGA_ID","correlationId":"$SAGA_ID","replyTo":"order.saga.replies","data":{"hello":"world"}}');
SQL

echo "5/5 read the Kafka record"
OUTPUT="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic smoke.commands --from-beginning --max-messages 1 --timeout-ms 60000 \
  --property print.headers=true --property print.key=true 2>/dev/null)"
echo "$OUTPUT"

fail=0
check() {
  if ! grep -q -- "$1" <<< "$OUTPUT"; then
    echo "FAIL: expected '$1'"
    fail=1
  fi
}
check "id:$EVENT_ID"
check "type:ValidateProducts"
check "order-42"
check "\"replyTo\":\"order.saga.replies\""
check "\"data\":{\"hello\":\"world\"}"

if [ "$fail" -eq 0 ]; then
  echo "PASS: outbox row became a Kafka record with headers id/type, key order-42 and an expanded JSON value"
else
  exit 1
fi
```


Then make them executable:

```bash
chmod +x infra/scripts/*.sh
```

- [ ] **Step 4: Validate the compose file**

```bash
cd infra && docker compose config -q
```

Expected: no output, exit code 0.

- [ ] **Step 5: Pull the images. This proves every tag exists**

```bash
cd infra && docker compose --profile ui pull
```

Expected: all images pulled. If `quay.io/debezium/connect:3.7.0.Final` or `apache/kafka:4.1.0` is not found, use the nearest published tag of the same minor line and keep Debezium at 3.7.x (it supports PostgreSQL 14 to 18).

- [ ] **Step 6: Start the stack**

```bash
cd infra && docker compose up -d --wait product-db order-db inventory-db kafka connect
docker compose up kafka-init
```

Expected: all services healthy; `kafka-init` exits with code 0.

- [ ] **Step 7: Check the topics**

```bash
cd infra && docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Expected: `product.commands`, `inventory.commands`, `order.saga.replies`, `order.events` and the same four with the `.DLT` suffix.

- [ ] **Step 8: Run the smoke test. It creates a throwaway database, inserts an outbox row and checks the Kafka record**

```bash
cd infra && ./scripts/smoke-outbox.sh
```

Expected last line: `PASS: outbox row became a Kafka record with headers id/type, key order-42 and an expanded JSON value`.

If it prints `FAIL`: look at the record printed above and at `curl -s localhost:8083/connectors/smoke-outbox/status`. The EventRouter settings in the connector JSON are the only part not yet run anywhere; fix them in all four JSON files (they are identical apart from the database, slot and prefix) and rerun.

- [ ] **Step 9: Commit**

```bash
git add infra
git commit -m "build(infra): docker-compose stack and Debezium outbox connectors with smoke test"
```

### Task 13: Library README
**Files:**
- Create: `common/README.md`

- [ ] **Step 1: Write the README (the consumer sketch is the pattern the Product, Inventory and Order plans implement; it is not compiled here)**

Create `common/README.md`:

````markdown
# shop-common

Shared infrastructure for the shop microservices. Add it as a dependency, set `shop.*` properties, write no infrastructure configuration.

```xml
<dependency>
    <groupId>com.shop</groupId>
    <artifactId>shop-common</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## What a service gets

| Bean | Purpose |
|---|---|
| `ConnectionPool` (single `ConnectionFactory`), `DatabaseClient` | R2DBC pool for PostgreSQL, created after Flyway |
| `TransactionalOperator`, `ReactiveTransactionManager` | wrap a `Mono`/`Flux` with `transactional(...)` |
| Flyway at startup | migrations in `classpath:db/common` (outbox, inbox) and `classpath:db/migration` (the service) |
| `OutboxWriter` | `write(topic, aggregateType, aggregateId, type, envelope)` inside the business transaction |
| `Inbox` | `once(consumerName, eventId, effect)` idempotence guard |
| `shopKafkaListenerContainerFactory` | manual ack, `read_committed`, retry, dead letter `<topic>.DLT` |
| `RetentionPurger` | scheduled batch purge of `processed_events` and `outbox_events` |

## Minimal `application.yml` of a service

```yaml
shop:
  r2dbc:
    host: localhost
    port: 5441
    database: product_db
    username: postgres
    password: postgres
    postgres:
      application-name: product-service
    pool:
      max-size: 20          # keep small, scale by instances
  kafka:
    bootstrap-servers: localhost:29092
    group-id: product-service
    concurrency: 3          # one consumer thread per assigned partition
  retention:
    inbox: 7d               # must exceed topic retention + redelivery window
    outbox: 3d
```

Every property has a production-oriented default; see `R2dbcProperties`, `KafkaConsumerProperties`, `FlywayProperties`, `TransactionProperties`, `RetentionProperties`.

## Consuming a topic (sketch used by the service plans)

```java
@Configuration
class ProductMessagingConfig {

    @Bean
    MessageDispatcher productCommandsDispatcher(Inbox inbox, TransactionalOperator transactions,
                                                Handler validateProducts) {
        HandlerRegistry registry = HandlerRegistry.builder()
                .on("ValidateProducts", validateProducts)
                .build();
        return new MessageDispatcher("product-service.product.commands", registry, inbox, transactions,
                Duration.ofSeconds(10));
    }
}

@Component
class ProductCommandsListener {

    private final MessageDispatcher dispatcher;

    ProductCommandsListener(MessageDispatcher productCommandsDispatcher) {
        this.dispatcher = productCommandsDispatcher;
    }

    @KafkaListener(topics = "product.commands", containerFactory = KafkaConsumerConfig.CONTAINER_FACTORY)
    void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        dispatcher.onMessage(record, acknowledgment);
    }
}
```

A `Handler` is `Function<InboundMessage, Mono<Void>>`: it does its database writes and `OutboxWriter.write(...)` calls inside the returned `Mono`; the dispatcher runs it in one transaction together with the inbox insert.

## Tests

```bash
mvn test      # unit tests, no Docker
mvn verify    # + integration tests (Testcontainers, Docker required)
```
````

- [ ] **Step 2: Commit**

```bash
git add common/README.md
git commit -m "docs(common): usage guide for services"
```

---

## Done when

- `cd common && mvn -B verify` is green (24 unit tests + 8 integration tests).
- `shop-common-1.0.0-SNAPSHOT` is in `~/.m2`.
- `infra/scripts/smoke-outbox.sh` prints `PASS`.

Then continue with plan 2 (Product service), which depends only on what this plan produced.
