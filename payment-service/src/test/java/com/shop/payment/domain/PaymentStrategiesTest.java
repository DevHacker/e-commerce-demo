package com.shop.payment.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import com.shop.payment.domain.strategy.CashOnDeliveryStrategy;
import com.shop.payment.domain.strategy.CreditCardStrategy;
import com.shop.payment.domain.strategy.PayPalStrategy;
import com.shop.payment.domain.strategy.PaymentStrategies;
import com.shop.payment.domain.strategy.PaymentStrategy;
import com.shop.payment.domain.strategy.RazorpayStrategy;
import com.shop.payment.domain.strategy.StripeStrategy;
import org.junit.jupiter.api.Test;

class PaymentStrategiesTest {

    private static List<PaymentStrategy> all() {
        return List.of(new CreditCardStrategy(), new CashOnDeliveryStrategy(), new PayPalStrategy(),
                new RazorpayStrategy(), new StripeStrategy());
    }

    @Test
    void everyMethodHasItsOwnStrategy() {
        PaymentStrategies strategies = new PaymentStrategies(all());

        for (PaymentMethod method : PaymentMethod.values()) {
            assertThat(strategies.find(method)).isPresent().get().extracting(PaymentStrategy::method).isEqualTo(method);
        }
        assertThat(strategies.supported()).containsExactlyInAnyOrder(PaymentMethod.values());
    }

    @Test
    void aMissingStrategyFailsAtStartup() {
        List<PaymentStrategy> withoutStripe = all().stream().filter(s -> s.method() != PaymentMethod.STRIPE).toList();

        assertThatThrownBy(() -> new PaymentStrategies(withoutStripe))
                .isInstanceOf(IllegalStateException.class).hasMessage("No strategy for STRIPE");
    }

    @Test
    void twoStrategiesForTheSameMethodFailAtStartup() {
        List<PaymentStrategy> twice = new java.util.ArrayList<>(all());
        twice.add(new StripeStrategy());

        assertThatThrownBy(() -> new PaymentStrategies(twice))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Two strategies for STRIPE");
    }

    @Test
    void methodNamesAreParsedLeniently() {
        assertThat(PaymentMethod.parse("credit_card")).contains(PaymentMethod.CREDIT_CARD);
        assertThat(PaymentMethod.parse(" Cash_On_Delivery ")).contains(PaymentMethod.CASH_ON_DELIVERY);
        assertThat(PaymentMethod.parse("bitcoin")).isEmpty();
        assertThat(PaymentMethod.parse(null)).isEmpty();
        assertThat(PaymentMethod.parse(" ")).isEmpty();
    }
}
