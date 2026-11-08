-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.
CREATE TABLE customers (
    id         UUID PRIMARY KEY,
    email      VARCHAR(200) NOT NULL,           -- stored lower-case
    first_name VARCHAR(100) NOT NULL,
    last_name  VARCHAR(100) NOT NULL,
    phone      VARCHAR(32),
    address    VARCHAR(500),
    active     BOOLEAN      NOT NULL DEFAULT TRUE,
    version    BIGINT       NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_customers_email UNIQUE (email)
);

CREATE INDEX idx_customers_created ON customers (created_at, id);
