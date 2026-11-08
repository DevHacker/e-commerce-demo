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
