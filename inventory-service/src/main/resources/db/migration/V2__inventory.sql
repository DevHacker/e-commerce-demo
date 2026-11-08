-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.

-- How many buckets ("shards") a product's stock is split into. Default 1; hot products get more.
CREATE TABLE product_stock_config (
    product_id   UUID PRIMARY KEY,
    bucket_count INT         NOT NULL DEFAULT 1 CHECK (bucket_count BETWEEN 1 AND 256),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- available = quantity on hand in this bucket, reserved = part of it promised to orders.
-- free stock of a bucket = available - reserved.
CREATE TABLE stock_buckets (
    product_id UUID   NOT NULL,
    bucket_no  INT    NOT NULL CHECK (bucket_no >= 0),
    available  BIGINT NOT NULL CHECK (available >= 0),
    reserved   BIGINT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    -- scratch value: quantity taken by the latest reservation UPDATE. It lets one atomic UPDATE
    -- report what it took (UPDATE ... RETURNING cannot see the old row before PostgreSQL 18's OLD).
    last_taken BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (product_id, bucket_no),
    CONSTRAINT ck_bucket_reserved_within_available CHECK (reserved <= available)
);

CREATE TABLE stock_reservations (
    order_id   UUID        NOT NULL,
    product_id UUID        NOT NULL,
    bucket_no  INT         NOT NULL,
    qty        BIGINT      NOT NULL CHECK (qty > 0),
    status     VARCHAR(16) NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, product_id, bucket_no)
);

-- Append-only history. delta = change of FREE stock: RESERVE < 0, RELEASE > 0, ADJUST either sign.
CREATE TABLE stock_movements (
    id          BIGSERIAL PRIMARY KEY,
    product_id  UUID        NOT NULL,
    order_id    UUID,
    delta       BIGINT      NOT NULL,
    type        VARCHAR(16) NOT NULL CHECK (type IN ('RESERVE', 'RELEASE', 'ADJUST')),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_stock_movements_product ON stock_movements (product_id, occurred_at);
CREATE INDEX idx_stock_movements_order ON stock_movements (order_id) WHERE order_id IS NOT NULL;
