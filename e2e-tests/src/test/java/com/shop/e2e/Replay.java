package com.shop.e2e;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Plays at-least-once delivery: publishes a reply Debezium already delivered once, byte for byte, with its headers. */
public final class Replay {

    private Replay() {
    }

    /** Republishes the first record of {@code topic} for {@code key} whose {@code type} header matches. Returns false if none. */
    public static boolean republish(String topic, String key, String type) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Stack.KAFKA);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-replay-" + UUID.randomUUID());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(500))) {
                    var header = rec.headers().lastHeader("type");
                    if (key.equals(rec.key()) && header != null && type.equals(new String(header.value(), StandardCharsets.UTF_8))) {
                        resend(rec);
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static void resend(ConsumerRecord<String, String> rec) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Stack.KAFKA);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(pp)) {
            producer.send(new ProducerRecord<>(rec.topic(), null, rec.key(), rec.value(), rec.headers())).get();
        } catch (Exception e) {
            throw new IllegalStateException("republish failed: " + e.getMessage(), e);
        }
    }
}
