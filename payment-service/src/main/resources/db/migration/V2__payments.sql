-- V1 is shop-common (outbox_events, processed_events). Service migrations start at V2.

-- One payment per order (the saga charges an order once); a failed attempt is kept, so a replayed
-- command gets the same answer instead of a second charge.
CREATE TABLE payments (
    id              UUID PRIMARY KEY,
    order_id        UUID           NOT NULL,
    method          VARCHAR(32)    NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL,
    status          VARCHAR(24)    NOT NULL
        CHECK (status IN ('CAPTURED', 'PENDING_ON_DELIVERY', 'FAILED', 'REFUNDED', 'VOIDED')),
    reference       VARCHAR(100),              -- the gateway's id for the charge
    failure_code    VARCHAR(64),
    failure_message VARCHAR(500),
    version         BIGINT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_payments_order UNIQUE (order_id)
);
