package com.shop.common.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Immutable lookup from message {@code type} to {@link Handler}. */
public final class HandlerRegistry {

    private final Map<String, Handler> handlers;

    private HandlerRegistry(Map<String, Handler> handlers) {
        this.handlers = Map.copyOf(handlers);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<Handler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public static final class Builder {

        private final Map<String, Handler> handlers = new HashMap<>();

        public Builder on(String type, Handler handler) {
            if (handlers.putIfAbsent(type, handler) != null) {
                throw new IllegalStateException("Handler already registered for type " + type);
            }
            return this;
        }

        public HandlerRegistry build() {
            return new HandlerRegistry(handlers);
        }
    }
}
