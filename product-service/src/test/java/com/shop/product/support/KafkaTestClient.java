package com.shop.product.support;

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

    /** Reads {@code topic} from the beginning until a record matches {@code wanted} or the timeout elapsed. */
    public java.util.Optional<ConsumerRecord<String, String>> readUntil(
            String topic, java.util.function.Predicate<ConsumerRecord<String, String>> wanted, Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (wanted.test(record)) {
                        return java.util.Optional.of(record);
                    }
                }
            }
        }
        return java.util.Optional.empty();
    }
}
