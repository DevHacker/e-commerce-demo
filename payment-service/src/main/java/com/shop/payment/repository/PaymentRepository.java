package com.shop.payment.repository;

import java.math.BigDecimal;
import java.util.UUID;

import com.shop.payment.domain.ConcurrentPaymentUpdateException;
import com.shop.payment.domain.Payment;
import com.shop.payment.domain.PaymentStatus;
import io.r2dbc.spi.Readable;
import reactor.core.publisher.Mono;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;

/** All SQL of the payment service. */
@Repository
public class PaymentRepository {

    private static final String COLUMNS =
            "id, order_id, method, amount, status, reference, failure_code, failure_message, version";

    private final DatabaseClient db;

    public PaymentRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Void> insert(Payment payment) {
        DatabaseClient.GenericExecuteSpec spec = db.sql("""
                        INSERT INTO payments (id, order_id, method, amount, status, reference, failure_code, failure_message)
                        VALUES (:id, :order, :method, :amount, :status, :reference, :code, :message)""")
                .bind("id", payment.id()).bind("order", payment.orderId()).bind("method", payment.method())
                .bind("amount", payment.amount()).bind("status", payment.status().name());
        spec = nullable(spec, "reference", payment.reference());
        spec = nullable(spec, "code", payment.failureCode());
        spec = nullable(spec, "message", payment.failureMessage());
        return spec.fetch().rowsUpdated().then();
    }

    public Mono<Payment> findByOrder(UUID orderId) {
        return db.sql("SELECT " + COLUMNS + " FROM payments WHERE order_id = :order")
                .bind("order", orderId).map(PaymentRepository::toPayment).one();
    }

    /** Optimistic: zero rows updated means someone else changed the payment first. */
    public Mono<Void> updateStatus(Payment payment, PaymentStatus newStatus) {
        return db.sql("""
                        UPDATE payments SET status = :status, version = version + 1, updated_at = now()
                         WHERE id = :id AND version = :version""")
                .bind("status", newStatus.name()).bind("id", payment.id()).bind("version", payment.version())
                .fetch().rowsUpdated()
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty()
                        : Mono.error(new ConcurrentPaymentUpdateException(payment.id())));
    }

    private static DatabaseClient.GenericExecuteSpec nullable(DatabaseClient.GenericExecuteSpec spec, String name, String value) {
        return value == null ? spec.bindNull(name, String.class) : spec.bind(name, value);
    }

    private static Payment toPayment(Readable row) {
        return new Payment(
                row.get("id", UUID.class), row.get("order_id", UUID.class), row.get("method", String.class),
                row.get("amount", BigDecimal.class), PaymentStatus.valueOf(row.get("status", String.class)),
                row.get("reference", String.class), row.get("failure_code", String.class),
                row.get("failure_message", String.class), row.get("version", Long.class));
    }
}
