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
