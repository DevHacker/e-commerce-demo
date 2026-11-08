# E-commerce microservices, phase 1 : design

Date : 2026-10-15
Statut : en attente de relecture utilisateur

## 1. Contexte et périmètre

Application e-commerce en microservices, 100 % réactive. Cible : très forte concurrence (millions d'utilisateurs simultanés).

**Phase 1 (cette spec)** : socle commun (`common`) + services **Product**, **Order**, **Inventory**. Order est l'orchestrateur de la saga.

**Phases suivantes** : Payment (phase 3, nouvelle étape de la saga), Customer (phase 4).

**Hors périmètre phase 1** : sécurité (authN/authZ), cache, service Customer, service Payment, UI.

## 2. Stack

| Élément | Choix |
|---|---|
| Langage | Java 21 |
| Framework | Spring Boot **4.1.1** (dernière stable vérifiée le 2026-10-15), Spring WebFlux |
| Accès données | R2DBC + **PostgreSQL 18** (`DatabaseClient`, pas de JPA). Debezium 3.7 supporte PostgreSQL 14 à 18. En PG 18, l'image Docker monte les données dans `/var/lib/postgresql`. |
| Migrations | Flyway (JDBC, uniquement au démarrage) |
| Messaging | Kafka ; consumers via **Spring Kafka** (listener containers, ack manuel, kafka-clients 4.1.x) ; producers via Debezium (CDC) |
| Pattern | Saga Orchestrator + Transactional Outbox + CDC Debezium (Outbox Event Router SMT) |
| Style | Fonctionnel : fonctions pures pour les décisions, `Mono`/`Flux` pour les effets |
| Tests | JUnit 5, Reactor Test, Testcontainers (Postgres, Kafka, Debezium Connect) |

Versions vérifiées le 2026-10-15 : Debezium **3.7.0.Final** ; Spring Kafka, Flyway (`flyway-core` + `flyway-database-postgresql`), r2dbc-postgresql, r2dbc-pool, Micrometer : versions **gérées par Spring Boot 4.1.1**, jamais épinglées à la main.

**reactor-kafka écarté** : projet discontinué (annonce du 20 mai 2025), retiré du Reactor BOM ; `ReactiveKafkaTemplate` et le binder réactif Spring Cloud Stream sont dépréciés. Les consumers utilisent Spring Kafka. WebFlux et R2DBC restent 100 % non bloquants ; seul le thread de listener Kafka appelle `block()` sur le `Mono` du handler.

## 3. Structure du repo

```
ecommerce/
  common/              librairie publiée (mvn install local, Artifactory plus tard)
  product-service/     projet Maven indépendant
  order-service/       projet Maven indépendant
  inventory-service/   projet Maven indépendant
  infra/               docker-compose + connecteurs Debezium (un JSON par service)
  docs/superpowers/specs/
```

- Pas de pom agrégateur. Chaque service dépend de `common` par version.
- `common` : `distributionManagement` en placeholder, plugins source et javadoc, version `1.0.0-SNAPSHOT`.
- Dossiers frères dans un même repo Git ; séparable plus tard.

## 4. Topologie Kafka

Une table `outbox_events` par service, lue par un connecteur Debezium par service (Outbox Event Router SMT, `route.by.field=topic`, clé = `aggregateid`, `type` en header, `id` en header).

| Topic | Producteur | Consommateur |
|---|---|---|
| `product.commands` | Order | Product |
| `inventory.commands` | Order | Inventory |
| `order.saga.replies` | Product, Inventory | Order |
| `order.events` | Order | (externes, futurs) |
| `<topic>.DLT` | `DeadLetterPublishingRecoverer` (Spring Kafka) | exploitation |

- Clé Kafka = `orderId`, donc ordre garanti par commande.
- `payload` = enveloppe JSON `{ sagaId, correlationId, replyTo, data }`. Debezium expose le JSON expansé comme valeur.
- Aucun service n'écrit directement dans Kafka, sauf le recoverer DLT de Spring Kafka.

## 5. Flux de la saga

Flux nominal :

1. `POST /orders` : Order, une transaction : `orders` (PENDING) + `order_items` + `saga_state` (STARTED) + historiques + outbox `ValidateProducts` vers `product.commands`.
2. Product : inbox + `validate` + outbox `ProductsValidated` (prix courants) ou `ProductsRejected` vers `order.saga.replies`.
3. Order : inbox + snapshot des prix + total + saga PRODUCTS_VALIDATED + outbox `ReserveStock` vers `inventory.commands`.
4. Inventory : inbox + réservation + outbox `StockReserved` ou `StockRejected` vers `order.saga.replies`.
5. Order : inbox + order CONFIRMED + saga COMPLETED + outbox `OrderConfirmed` vers `order.events`.

Échecs :

- `ProductsRejected` ou `StockRejected` : order `REJECTED`, saga REJECTED, outbox `OrderRejected`. Rien à compenser (validation = lecture seule ; réservation échouée = rollback).
- `POST /orders/{id}/cancel` (uniquement `CONFIRMED`) : order CANCELLING, saga COMPENSATING, outbox `ReleaseStock`. Réponse `StockReleased` : order CANCELLED, saga COMPENSATED, outbox `OrderCancelled`.
- **Timeout** : balayeur planifié dans Order. Saga non terminale dont `updated_at` dépasse le délai configurable : passage en compensation (`ReleaseStock` si une réservation a pu avoir lieu), sinon REJECTED.

Machine à états (`saga_state.state`) : `STARTED → PRODUCTS_VALIDATED → STOCK_RESERVED → COMPLETED` ; branches `REJECTED`, `COMPENSATING → COMPENSATED`.

Cœur : `transition(SagaState, Reply) → Transition(nouvelEtat, Optional<Command>, changementStatut, entréesHistorique)`. Fonction pure. Le handler applique le résultat en une transaction.

## 6. Modèle de données

Toutes les bases (`product_db`, `order_db`, `inventory_db`) ont les deux tables ci-dessous, créées par la migration `V1` fournie par `common` (`classpath:db/common`).

```sql
CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    aggregatetype VARCHAR(100) NOT NULL,
    aggregateid VARCHAR(255) NOT NULL,
    type VARCHAR(150) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- `consumer_name` = `<service>.<topic>`. Insert `ON CONFLICT DO NOTHING` dans la transaction de l'effet métier ; 0 ligne = doublon, ignoré, offset commité.
- Purge planifiée par lots : `processed_events` (rétention > rétention Kafka + fenêtre de redelivery), `outbox_events` (lignes déjà lues par Debezium, rétention configurable).

**order_db** : `orders` (id, customer_ref, status, total, idempotency_key UNIQUE, version, timestamps), `order_items` (order_id, product_id, qty, unit_price snapshot), `saga_state` (saga_id = order_id, state, current_step, version, updated_at), `order_status_history` (id, order_id, from_status, to_status, reason, trigger_message_id, occurred_at), `saga_step_history` (id, saga_id, step, direction `FORWARD|COMPENSATE`, outcome, message_id, detail JSONB, occurred_at).

**product_db** : `products` (id, sku UNIQUE, name, price, active, version).

**inventory_db** : `product_stock_config` (product_id, bucket_count), `stock_buckets` (product_id, bucket_no, available, reserved), `stock_reservations` (order_id, product_id, bucket_no, qty, status `RESERVED|RELEASED`), `stock_movements` (product_id, order_id, delta, type `RESERVE|RELEASE|ADJUST`, occurred_at).

Les historiques sont append-only et écrits dans la même transaction que la transition qu'ils décrivent.

## 7. Concurrence et verrouillage

| Table | Stratégie | Raison |
|---|---|---|
| `stock_buckets` | UPDATE conditionnel atomique | Verrou de ligne limité à un statement ; pas de connexion retenue ; pas de retry storm |
| `orders`, `saga_state` | Optimiste (`version`), retry borné | 1 saga par commande, messages ordonnés par clé ; conflit rare |
| `products` | Optimiste (`version`) | Lectures dominantes, écritures admin rares |
| `processed_events` | PK unique | Idempotence sans verrou |

Le verrou pessimiste (`FOR UPDATE`) est écarté : en R2DBC, verrou tenu = connexion du pool tenue, ce qui sature le pool sous charge.

**Réservation avec buckets**

- Sémantique : `available` = quantité en stock du bucket, `reserved` = part promise aux commandes ; stock libre = `available - reserved`.
- Bucket de départ = `hash(orderId) mod bucket_count`, puis parcours croissant avec retour à 0 (`BucketScan`, fonction pure).
- Prise par bucket en un seul `UPDATE` atomique : `SET reserved = reserved + LEAST(available - reserved, :besoin), last_taken = LEAST(...) ... WHERE available > reserved RETURNING last_taken`. `last_taken` est une colonne de travail qui permet de connaître la quantité prise sans `OLD` (PostgreSQL 18 seulement). Un bucket sans stock libre ne matche aucune ligne : ni écriture, ni verrou. Une commande peut s'étaler sur plusieurs buckets.
- Produits traités dans l'ordre trié par `product_id` (`StockNeeds`) : pas de deadlock entre produits. Au sein d'un produit, le parcours circulaire ne verrouille pas strictement en ordre croissant (retour à 0) : un deadlock exige que deux commandes débordent chacune sur le bucket suivant, donc un produit presque épuisé. PostgreSQL avorte alors l'une d'elles, la transaction du message est annulée et le consumer la rejoue (`DefaultErrorHandler`).
- Besoin non couvert après tous les buckets : **annulation dans la même transaction** (chaque quantité prise est rendue par `UPDATE reserved = reserved - qty`) ; aucune ligne `stock_reservations` ni `stock_movements` n'est écrite avant que tous les produits soient couverts. La transaction du message commit alors seulement inbox + outbox `StockRejected`. Aucune réservation fantôme. (Une seconde transaction `REQUIRES_NEW` a été écartée : elle retient deux connexions du pool par message et exige un pool dimensionné en conséquence.)
- `ReleaseStock` rend exactement aux buckets enregistrés dans `stock_reservations` (`UPDATE ... SET status = 'RELEASED' ... RETURNING`) ; sans réservation `RESERVED`, no-op qui répond `StockReleased`.
- Défaut `bucket_count = 1` ; produit chaud = N (ex. 16), configurable par produit via `PUT /stock/{productId}`. `PUT` fixe la quantité en stock : le stock libre devient `quantité - réservé`, réparti équitablement ; refus (409) si la quantité est inférieure au réservé ou si réduire le nombre de buckets supprimerait un bucket qui porte des réservations.
- Disponibilité affichée = somme des buckets (cohérence éventuelle acceptée).

## 8. Librairie `common`

Exposée en auto-configuration Spring Boot (`META-INF/spring/...AutoConfiguration.imports`) avec `@ConfigurationProperties`. Les services ne contiennent aucune config d'infrastructure.

**Fichiers de configuration**

- `R2dbcConnectionConfig` (prefix `shop.r2dbc`) : `ConnectionFactory` PostgreSQL + `ConnectionPool` : `initialSize`, `maxSize` (petit, on scale par instances), `maxIdleTime`, `maxLifeTime`, `maxAcquireTime`, `maxCreateConnectionTime`, `acquireRetry`, `backgroundEvictionInterval`, `validationDepth REMOTE` (+ `validationQuery` optionnelle), warmup au démarrage (échec rapide si la base est injoignable), métriques Micrometer, nom du pool. Options Postgres : `connectTimeout`, `tcpKeepAlive`, `application_name`, `statement_timeout`, cache de prepared statements, SSL.
- `TransactionConfig` (fichier séparé) : `R2dbcTransactionManager` + `TransactionalOperator` (propagation `REQUIRED`, timeout).
- `FlywayConfig` : DataSource JDBC éphémère (1 connexion, fermée après migration), exécutée avant l'ouverture du pool R2DBC. Locations : `classpath:db/common` + `classpath:db/migration`.

**Briques fonctionnelles**

- `Envelope<T>` (sagaId, correlationId, replyTo, data). Le `type` du message voyage en colonne `outbox_events.type` et en header Kafka `type`, pas dans le JSON.
- `OutboxWriter.write(topic, aggregateType, aggregateId, type, envelope)` : `Mono<UUID>` (id de la ligne = header Kafka `id`), via `DatabaseClient`, dans la transaction courante.
- `Inbox.once(consumerName, eventId, effect)` : `Mono<Boolean>`, `false` si doublon (effet non exécuté).
- `Handler = Function<InboundMessage, Mono<Void>>` : tous les effets (base, outbox) sont dans le `Mono`. Handlers enregistrés dans un `HandlerRegistry` (par `type`).
- `RetentionPurger` : purge par lots de `processed_events` et `outbox_events`, planifiée.
- Gauges Micrometer du pool R2DBC (`MeterBinder`). `JsonCodec` repose sur Jackson 3 (`tools.jackson`).
- `KafkaConsumerConfig` : `ConcurrentKafkaListenerContainerFactory` (`AckMode.MANUAL`, `enable.auto.commit=false`, `isolation.level=read_committed`, `concurrency` configurable, un thread par partition assignée) + `DefaultErrorHandler` (`ExponentialBackOff`) + `DeadLetterPublishingRecoverer` (topic `<topic>.DLT`, même partition). Erreurs de désérialisation et exceptions non retentables (`NonRetryableException`) : DLT direct.
- `MessageDispatcher` : listener unique par service. Lit `type` (header) et `id` (header), choisit le `Handler` dans le `HandlerRegistry`, exécute `TransactionalOperator.transactional(Inbox.once(consumerName, eventId, () -> handler.apply(msg)))` puis `block()` sur le thread du listener, puis `ack.acknowledge()` **après** commit. Ordre par partition garanti par le container (un thread par partition).

## 9. Services

**Product**

- API : `POST /products`, `GET /products/{id}`, `GET /products` (paginé), `PUT /products/{id}` (version optimiste, `409` en cas de conflit).
- Handler `ValidateProducts` : `validate(request, Map<ProductId, Product>) → Either<Rejection, Validated>`. Rejets : `NOT_FOUND`, `INACTIVE` par produit.

**Order**

- API : `POST /orders` (header `Idempotency-Key`) → `202` + `orderId` ; `GET /orders/{id}` ; `GET /orders/{id}/history` ; `POST /orders/{id}/cancel`.
- Statuts : `PENDING`, `CONFIRMED`, `REJECTED`, `CANCELLING`, `CANCELLED`.
- Handlers sur `order.saga.replies` : `ProductsValidated`, `ProductsRejected`, `StockReserved`, `StockRejected`, `StockReleased`.
- Événements domaine : `OrderCreated`, `OrderConfirmed`, `OrderRejected`, `OrderCancelled`.

**Inventory**

- API admin : `PUT /stock/{productId}` (quantité + `bucketCount`), `GET /stock/{productId}`.
- Handlers : `ReserveStock`, `ReleaseStock`. Le stock s'initialise par l'API admin ; Inventory ne dépend pas d'événements Product.

## 10. Gestion des erreurs

- Échec métier : événement de réponse, jamais DLT.
- Échec technique (DB indisponible, timeout) : retry avec backoff exponentiel (`DefaultErrorHandler`), puis `<topic>.DLT` (`DeadLetterPublishingRecoverer`).
- Message illisible (désérialisation) ou `NonRetryableException` : DLT direct.
- Offset commité uniquement après commit de la transaction DB.

## 11. Tests

- Unitaires : `validate`, `transition`, allocation de buckets, `Inbox.once`.
- Intégration (Testcontainers) : Postgres + Kafka + Debezium Connect. Scénarios e2e : flux nominal, rejet produit, rejet stock, annulation avec compensation, redelivery du même message (idempotence), stock concurrent sur un produit à plusieurs buckets.

## 12. Décisions écartées

- Orchestrateur séparé ou embarqué en librairie : écartés au profit d'Order orchestrateur (moins de pièces).
- Un topic par type d'événement ou un topic unique : écartés au profit des topics par rôle.
- reactor-kafka, `ReactiveKafkaConsumerTemplate` et le binder réactif Spring Cloud Stream : écartés (discontinués ou dépréciés, hors Reactor BOM). Spring Cloud Stream en général : écarté (commit d'offset moins maîtrisable vis-à-vis de la transaction DB). Choix retenu : Spring Kafka + handlers fonctionnels + `block()` sur le thread de listener.
- Wrapper réactif maison sur `kafka-clients` : écarté (brique critique à maintenir soi-même).
- Verrou pessimiste et optimiste pur sur le stock : écartés au profit de l'UPDATE conditionnel sur buckets.
