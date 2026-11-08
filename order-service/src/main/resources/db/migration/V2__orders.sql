-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.

CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_ref    VARCHAR(100) NOT NULL,
    status          VARCHAR(16)  NOT NULL
        CHECK (status IN ('PENDING', 'CONFIRMED', 'REJECTED', 'CANCELLING', 'CANCELLED')),
    total           NUMERIC(14, 2),            -- set when the products are validated (prices snapshot)
    payment_method  VARCHAR(32)  NOT NULL
        CHECK (payment_method IN ('CREDIT_CARD', 'CASH_ON_DELIVERY', 'PAYPAL', 'RAZORPAY', 'STRIPE')),
    payment_details JSONB        NOT NULL DEFAULT '{}'::jsonb,   -- tokens and references only, never card numbers
    idempotency_key VARCHAR(100) NOT NULL,
    version         BIGINT       NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_orders_idempotency_key UNIQUE (idempotency_key)
);

CREATE TABLE order_items (
    order_id   UUID           NOT NULL REFERENCES orders (id),
    product_id UUID           NOT NULL,
    quantity   INT            NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2),                 -- snapshot taken from the ProductsValidated reply
    PRIMARY KEY (order_id, product_id)
);

-- One row per order; saga_id = order_id.
CREATE TABLE saga_state (
    saga_id      UUID PRIMARY KEY REFERENCES orders (id),
    state        VARCHAR(24) NOT NULL
        CHECK (state IN ('STARTED', 'PRODUCTS_VALIDATED', 'STOCK_RESERVED', 'COMPLETED', 'REJECTED', 'COMPENSATING', 'COMPENSATED')),
    current_step VARCHAR(32) NOT NULL,
    version      BIGINT      NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- the timeout sweeper looks for stale non-terminal sagas
CREATE INDEX idx_saga_state_pending ON saga_state (updated_at)
    WHERE state IN ('STARTED', 'PRODUCTS_VALIDATED', 'STOCK_RESERVED', 'COMPENSATING');

-- Append-only histories, written in the same transaction as the transition they describe.
CREATE TABLE order_status_history (
    id                 BIGSERIAL PRIMARY KEY,
    order_id           UUID        NOT NULL REFERENCES orders (id),
    from_status        VARCHAR(16),
    to_status          VARCHAR(16) NOT NULL,
    reason             VARCHAR(200),
    trigger_message_id UUID,
    occurred_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_order_status_history_order ON order_status_history (order_id, id);

CREATE TABLE saga_step_history (
    id          BIGSERIAL PRIMARY KEY,
    saga_id     UUID        NOT NULL REFERENCES saga_state (saga_id),
    step        VARCHAR(32) NOT NULL,
    direction   VARCHAR(16) NOT NULL CHECK (direction IN ('FORWARD', 'COMPENSATE')),
    outcome     VARCHAR(32) NOT NULL,
    message_id  UUID,
    detail      JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_saga_step_history_saga ON saga_step_history (saga_id, id);
