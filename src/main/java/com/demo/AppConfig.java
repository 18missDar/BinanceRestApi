package com.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {
    @Value("${EVENT_SYMBOL}")
    private String eventSymbol;

    @Value("${LIMIT_COUNT}")
    private int limitCount;

    public String getEventSymbol() {
        return eventSymbol;
    }

    public int getLimitCount() {
        return limitCount;
    }
}