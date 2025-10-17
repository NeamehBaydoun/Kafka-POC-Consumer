package com.mdsl.kafka.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum Status {
    ACTIVE("ACTIVE"),
    INACTIVE("INACTIVE");
    @Getter
    private final String name;
}
