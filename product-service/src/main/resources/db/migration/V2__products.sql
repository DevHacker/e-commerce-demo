-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.
CREATE TABLE products (
    id         UUID PRIMARY KEY,
    sku        VARCHAR(64)    NOT NULL,
    name       VARCHAR(200)   NOT NULL,
    price      NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    active     BOOLEAN        NOT NULL DEFAULT TRUE,
    version    BIGINT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_products_sku UNIQUE (sku)
);

CREATE INDEX idx_products_created ON products (created_at, id);
