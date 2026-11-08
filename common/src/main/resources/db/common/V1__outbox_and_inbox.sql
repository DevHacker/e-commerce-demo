-- Shared by every service database: inbox (idempotent consumers) and transactional outbox (Debezium CDC).

CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX idx_processed_events_processed_at ON processed_events (processed_at);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    aggregatetype VARCHAR(100) NOT NULL,
    aggregateid VARCHAR(255) NOT NULL,
    type VARCHAR(150) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_events_occurred_at ON outbox_events (occurred_at);
