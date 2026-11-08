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
