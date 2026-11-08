package com.shop.common.messaging;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

/** Thin JSON facade (Jackson 3). Parsing failures are not retryable. */
public class JsonCodec {

    private final JsonMapper mapper;

    public JsonCodec(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JacksonException e) {
            throw new NonRetryableException("Cannot serialise " + value.getClass().getName(), e);
        }
    }

    public <T> T read(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JacksonException e) {
            throw new NonRetryableException("Cannot parse " + type.getSimpleName() + ": " + e.getOriginalMessage(), e);
        }
    }

    public <T> Envelope<T> readEnvelope(String json, Class<T> dataType) {
        try {
            JavaType type = mapper.getTypeFactory().constructParametricType(Envelope.class, dataType);
            return mapper.readValue(json, type);
        } catch (JacksonException e) {
            throw new NonRetryableException(
                    "Cannot parse Envelope<" + dataType.getSimpleName() + ">: " + e.getOriginalMessage(), e);
        }
    }
}
