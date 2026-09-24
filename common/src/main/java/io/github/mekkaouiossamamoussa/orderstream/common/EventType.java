package io.github.mekkaouiossamamoussa.orderstream.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum EventType {
    @JsonProperty("OrderCreated")
    ORDER_CREATED("OrderCreated"),

    @JsonProperty("OrderStatusChanged")
    ORDER_STATUS_CHANGED("OrderStatusChanged");

    private final String jsonName;

    EventType(String jsonName) {
        this.jsonName = jsonName;
    }

    public String jsonName() {
        return jsonName;
    }
}
