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
