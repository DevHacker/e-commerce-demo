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
