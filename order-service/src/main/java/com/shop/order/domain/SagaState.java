package com.shop.order.domain;

public enum SagaState {
    STARTED, PRODUCTS_VALIDATED, STOCK_RESERVED, COMPLETED, REJECTED, COMPENSATING, COMPENSATED;

    /** A terminal saga never changes again (a completed order can still be cancelled: that starts COMPENSATING). */
    public boolean terminal() {
        return this == REJECTED || this == COMPENSATED;
    }

    /** States the timeout sweeper watches. */
    public boolean pending() {
        return this == STARTED || this == PRODUCTS_VALIDATED || this == STOCK_RESERVED || this == COMPENSATING;
    }
}
