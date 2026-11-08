# Payment (Strategy) and Customer: design addendum

Completes the system of `2026-10-15-ecommerce-phase1-design.md` (the spec's "phase 3" and "phase 4"). Decisions below
replace the parts of that spec and of plans 4 and 5 they contradict.

## 1. Saga with a payment step

```
STARTED --ProductsValidated--> PRODUCTS_VALIDATED --StockReserved--> STOCK_RESERVED --PaymentCompleted--> COMPLETED
   |                                 |                                    |                                  |
   +--ProductsRejected--> REJECTED   +--StockRejected--> REJECTED         |                                cancel
                                                                          |                                  |
  PaymentFailed -> COMPENSATING(ReleaseStock) --StockReleased--> COMPENSATED   (order PENDING -> REJECTED)   |
  cancel        -> COMPENSATING(RefundPayment) --PaymentRefunded--> COMPENSATING(ReleaseStock)  <------------+
                   --StockReleased--> COMPENSATED                                   (order CANCELLING -> CANCELLED)
```

* `STOCK_RESERVED` is now a stored state (plan 4 decision 1 no longer holds): the saga waits there for Payment.
* **Order of compensation: refund first, then release the stock.** Stock goes back on sale only once the money is
  safe, never the other way round. Payment's refund is idempotent and answers even when there is nothing to refund
  (`refunded=false`), so the saga always finishes.
* A failed payment is a rejection, not a cancellation: the order stays `PENDING` while the stock is released, then
  ends `REJECTED` with reason `PAYMENT_FAILED` (`StockReleased` on an order that is not `CANCELLING` means this).
* Timeouts: `STARTED` rejects; `PRODUCTS_VALIDATED` releases the stock; `STOCK_RESERVED` refunds first (the charge may
  have happened) then releases; `COMPENSATING` sends its current command again.
* Messages: `payment.commands` carries `ProcessPayment {amount, method, details}` and `RefundPayment {reason}`;
  replies on `order.saga.replies`: `PaymentCompleted {paymentId, method, status, reference}`,
  `PaymentFailed {code, message}`, `PaymentRefunded {refunded, paymentId}`. `amount` is the price snapshot total.
* `POST /orders` now requires `payment: {method, details}`. The order stores the method and the details (tokens and
  references only, never card numbers). Method names are case-insensitive.

## 2. Payment service: Strategy pattern

`PaymentStrategy` is the strategy; `PaymentStrategies` is the registry (it refuses to start if a method has no
strategy or two). `PaymentService` contains no `if method == ...`: it looks up the strategy and calls it.

| Method | Required details | Behaviour (simulated gateway) |
|---|---|---|
| `CREDIT_CARD` | `cardToken` | `tok_declined` -> `CARD_DECLINED`, `tok_insufficient_funds` -> `INSUFFICIENT_FUNDS`, else `CAPTURED` |
| `CASH_ON_DELIVERY` | `deliveryAddress` | accepted as `PENDING_ON_DELIVERY`; above 500.00 -> `COD_LIMIT_EXCEEDED`; refund = `VOIDED` (nothing collected) |
| `PAYPAL` | `payerEmail` | must contain `@`; `declined@...` -> `PAYER_DECLINED`, else `CAPTURED` |
| `RAZORPAY` | `razorpayPaymentId` (`pay_...`), `razorpaySignature` | signature `invalid` -> `SIGNATURE_MISMATCH`, else `CAPTURED` |
| `STRIPE` | `paymentMethodId` (`pm_...`) | Stripe test ids `pm_card_chargeDeclined`, `pm_card_chargeDeclinedInsufficientFunds` refused, else `CAPTURED` |

**The gateways are simulated**: no credentials, no network, deterministic. To go live, replace the body of one
strategy with the provider's SDK call (they already return a `Mono`); nothing else changes. To add a method: add an enum
value and a strategy `@Component`; the startup check tells you if you forgot one.

Also: one payment per order (`UNIQUE(order_id)`), failed attempts are kept, so a replayed `ProcessPayment` gets the same
answer and nobody is charged twice. Statuses: `CAPTURED`, `PENDING_ON_DELIVERY`, `FAILED`, `REFUNDED`, `VOIDED`.
HTTP (read-only): `GET /payments/{orderId}`, `GET /payments/methods`.

## 3. Customer service

A standalone registry: `POST/GET/PUT /customers`, e-mail unique (case-insensitive) and immutable, optimistic `version`
on update, pagination. It neither publishes nor consumes messages yet. **Not done on purpose**: `customerRef` on an order
is still an opaque string; Order does not check that the customer exists. That would add a `ValidateCustomer` step to the
saga and a `customer.events` stream, and is the natural next increment.

## 4. Infrastructure

Ports: product 8081 / db 5441, order 8080 / 5442, inventory 8082 / 5443, payment 8084 / 5444, customer 8085 / 5445,
Kafka 29092, Connect 8083, Kafka UI 8089. New topic `payment.commands` (+ `.DLT`); new connector `payment-outbox`.

## 5. Verification status

Unit tests and the integration tests of each service ran green against PostgreSQL 16 and an embedded Kafka broker
(no Docker daemon was available): Order 29 + 16, Payment 9 + 24, Customer 5 + 7. Never run: Testcontainers, PostgreSQL 18,
Debezium, and the `e2e-tests` module (compiles only).
