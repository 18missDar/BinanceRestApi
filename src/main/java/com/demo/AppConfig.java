package com.demo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    private String eventSymbol;

    private int limitCount;

    private int updateSpeed;

    public String getEventSymbol() {
        return eventSymbol;
    }

    public void setEventSymbol(String eventSymbol) {
        this.eventSymbol = eventSymbol;
    }

    public void setLimitCount(int limitCount) {
        this.limitCount = limitCount;
    }

    public int getUpdateSpeed() {
        return updateSpeed;
    }

    public void setUpdateSpeed(int updateSpeed) {
        this.updateSpeed = updateSpeed;
    }

    public int getLimitCount() {
        return limitCount;
    }

    public AppConfig() {
    }
}