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
