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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

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
    public static final String DLT_SUFFIX = "-dlt";

    @Bean
    public ConsumerFactory<String, String> shopConsumerFactory(KafkaConsumerProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
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
