package com.shop.payment.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import com.shop.payment.domain.strategy.CashOnDeliveryStrategy;
import com.shop.payment.domain.strategy.ChargeRequest;
import com.shop.payment.domain.strategy.ChargeResult;
import com.shop.payment.domain.strategy.ChargeResult.Approved;
import com.shop.payment.domain.strategy.ChargeResult.Declined;
import com.shop.payment.domain.strategy.CreditCardStrategy;
import com.shop.payment.domain.strategy.PayPalStrategy;
import com.shop.payment.domain.strategy.PaymentStrategy;
import com.shop.payment.domain.strategy.RazorpayStrategy;
import com.shop.payment.domain.strategy.RefundRequest;
import com.shop.payment.domain.strategy.RefundResult;
import com.shop.payment.domain.strategy.StripeStrategy;
import org.junit.jupiter.api.Test;

/** What each strategy requires, accepts and refuses. */
class StrategyRulesTest {

    private static ChargeResult charge(PaymentStrategy strategy, String amount, Map<String, String> details) {
        return strategy.charge(new ChargeRequest(UUID.randomUUID(), new BigDecimal(amount), details)).block();
    }

    private static RefundResult refund(PaymentStrategy strategy, PaymentStatus status) {
        return strategy.refund(new RefundRequest("ref", new BigDecimal("10.00"), status)).block();
    }

    // ---- credit card -----------------------------------------------------------------------------

    @Test
    void creditCardNeedsACardTokenAndRefusesTheDeclineTokens() {
        CreditCardStrategy card = new CreditCardStrategy();

        assertThat(card.validate(Map.of())).containsExactly("cardToken is required for CREDIT_CARD");
        assertThat(card.validate(Map.of("cardToken", " "))).hasSize(1);
        assertThat(card.validate(Map.of("cardToken", "tok_visa"))).isEmpty();

        assertThat(charge(card, "10.00", Map.of("cardToken", "tok_visa")))
                .isInstanceOfSatisfying(Approved.class, a -> {
                    assertThat(a.status()).isEqualTo(PaymentStatus.CAPTURED);
                    assertThat(a.reference()).startsWith("cc_");
                });
        assertThat(charge(card, "10.00", Map.of("cardToken", "tok_declined")))
                .isEqualTo(new Declined("CARD_DECLINED", "The card was declined"));
        assertThat(charge(card, "10.00", Map.of("cardToken", "tok_insufficient_funds")))
                .isInstanceOfSatisfying(Declined.class, d -> assertThat(d.code()).isEqualTo("INSUFFICIENT_FUNDS"));
        assertThat(refund(card, PaymentStatus.CAPTURED)).isInstanceOf(RefundResult.Refunded.class);
    }

    // ---- cash on delivery --------------------------------------------------------------------------

    @Test
    void cashOnDeliveryIsAcceptedPendingCollectionUpToTheLimit() {
        CashOnDeliveryStrategy cod = new CashOnDeliveryStrategy();
        Map<String, String> address = Map.of("deliveryAddress", "12 rue des Lilas, Abidjan");

        assertThat(cod.validate(Map.of())).containsExactly("deliveryAddress is required for CASH_ON_DELIVERY");
        assertThat(charge(cod, "500.00", address)).isInstanceOfSatisfying(Approved.class,
                a -> assertThat(a.status()).isEqualTo(PaymentStatus.PENDING_ON_DELIVERY));
        assertThat(charge(cod, "500.01", address)).isInstanceOfSatisfying(Declined.class,
                d -> assertThat(d.code()).isEqualTo("COD_LIMIT_EXCEEDED"));
        assertThat(refund(cod, PaymentStatus.PENDING_ON_DELIVERY)).isInstanceOf(RefundResult.Voided.class);
    }

    // ---- PayPal ----------------------------------------------------------------------------------------

    @Test
    void payPalNeedsAnEmailAndRefusesADecliningPayer() {
        PayPalStrategy paypal = new PayPalStrategy();

        assertThat(paypal.validate(Map.of())).containsExactly("payerEmail is required for PAYPAL");
        assertThat(paypal.validate(Map.of("payerEmail", "not-an-email"))).containsExactly("payerEmail must be an e-mail address");
        assertThat(paypal.validate(Map.of("payerEmail", "fidele@example.com"))).isEmpty();

        assertThat(charge(paypal, "10.00", Map.of("payerEmail", "fidele@example.com")))
                .isInstanceOfSatisfying(Approved.class, a -> assertThat(a.reference()).startsWith("pp_"));
        assertThat(charge(paypal, "10.00", Map.of("payerEmail", "declined@example.com")))
                .isInstanceOfSatisfying(Declined.class, d -> assertThat(d.code()).isEqualTo("PAYER_DECLINED"));
        assertThat(refund(paypal, PaymentStatus.CAPTURED)).isInstanceOf(RefundResult.Refunded.class);
    }

    // ---- Razorpay --------------------------------------------------------------------------------------

    @Test
    void razorpayNeedsAPaymentIdAndASignatureThatVerifies() {
        RazorpayStrategy razorpay = new RazorpayStrategy();
        Map<String, String> good = Map.of("razorpayPaymentId", "pay_29QQoUBi66xm2f", "razorpaySignature", "9ef4dffbfd84f1");

        assertThat(razorpay.validate(Map.of())).hasSize(2);
        assertThat(razorpay.validate(Map.of("razorpayPaymentId", "abc", "razorpaySignature", "s")))
                .containsExactly("razorpayPaymentId must start with pay_");
        assertThat(razorpay.validate(good)).isEmpty();

        assertThat(charge(razorpay, "10.00", good)).isInstanceOfSatisfying(Approved.class,
                a -> assertThat(a.reference()).isEqualTo("pay_29QQoUBi66xm2f"));
        assertThat(charge(razorpay, "10.00", Map.of("razorpayPaymentId", "pay_1", "razorpaySignature", "invalid")))
                .isInstanceOfSatisfying(Declined.class, d -> assertThat(d.code()).isEqualTo("SIGNATURE_MISMATCH"));
        assertThat(refund(razorpay, PaymentStatus.CAPTURED)).isInstanceOf(RefundResult.Refunded.class);
    }

    // ---- Stripe ------------------------------------------------------------------------------------------

    @Test
    void stripeNeedsAPaymentMethodIdAndRefusesTheDeclineTestIds() {
        StripeStrategy stripe = new StripeStrategy();

        assertThat(stripe.validate(Map.of())).containsExactly("paymentMethodId is required for STRIPE");
        assertThat(stripe.validate(Map.of("paymentMethodId", "card_123"))).containsExactly("paymentMethodId must start with pm_");
        assertThat(stripe.validate(Map.of("paymentMethodId", "pm_card_visa"))).isEmpty();

        assertThat(charge(stripe, "10.00", Map.of("paymentMethodId", "pm_card_visa")))
                .isInstanceOfSatisfying(Approved.class, a -> assertThat(a.reference()).startsWith("ch_"));
        assertThat(charge(stripe, "10.00", Map.of("paymentMethodId", "pm_card_chargeDeclined")))
                .isInstanceOfSatisfying(Declined.class, d -> assertThat(d.code()).isEqualTo("CARD_DECLINED"));
        assertThat(charge(stripe, "10.00", Map.of("paymentMethodId", "pm_card_chargeDeclinedInsufficientFunds")))
                .isInstanceOfSatisfying(Declined.class, d -> assertThat(d.code()).isEqualTo("INSUFFICIENT_FUNDS"));
        assertThat(refund(stripe, PaymentStatus.CAPTURED)).isInstanceOf(RefundResult.Refunded.class);
    }
}
