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
