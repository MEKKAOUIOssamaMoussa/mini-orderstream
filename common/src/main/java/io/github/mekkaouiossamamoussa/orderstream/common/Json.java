package io.github.mekkaouiossamamoussa.orderstream.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.UncheckedIOException;

public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Json() {
    }

    public static String toJson(OrderEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static OrderEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, OrderEvent.class);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
