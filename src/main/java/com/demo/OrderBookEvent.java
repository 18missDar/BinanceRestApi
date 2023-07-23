package com.demo;

import java.util.List;
import java.util.Objects;

public class OrderBookEvent {
    private String eventType;
    private long eventTime;
    private String symbol;
    private long firstUpdateId;
    private long finalUpdateId;
    private List<PriceQuantityPair> bids;
    private List<PriceQuantityPair> asks;

    // Constructor, getters, and setters

    public static class PriceQuantityPair {
        private String price;
        private String quantity;

        // Constructor, getters, and setters

        public PriceQuantityPair(String price, String quantity) {
            this.price = price;
            this.quantity = quantity;
        }

        public String getPrice() {
            return price;
        }

        public void setPrice(String price) {
            this.price = price;
        }

        public String getQuantity() {
            return quantity;
        }

        public void setQuantity(String quantity) {
            this.quantity = quantity;
        }

        @Override
        public String toString() {
            return "[ \"" + price + "\"" +
                    ", \"" + quantity + "\"" +
                    "]";
        }
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public long getFirstUpdateId() {
        return firstUpdateId;
    }

    public void setFirstUpdateId(long firstUpdateId) {
        this.firstUpdateId = firstUpdateId;
    }

    public long getFinalUpdateId() {
        return finalUpdateId;
    }

    public void setFinalUpdateId(long finalUpdateId) {
        this.finalUpdateId = finalUpdateId;
    }

    public List<PriceQuantityPair> getBids() {
        return bids;
    }

    public void setBids(List<PriceQuantityPair> bids) {
        this.bids = bids;
    }

    public List<PriceQuantityPair> getAsks() {
        return asks;
    }

    public void setAsks(List<PriceQuantityPair> asks) {
        this.asks = asks;
    }

    public OrderBookEvent(String eventType, long eventTime, String symbol, long firstUpdateId, long finalUpdateId, List<PriceQuantityPair> bids, List<PriceQuantityPair> asks) {
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.symbol = symbol;
        this.firstUpdateId = firstUpdateId;
        this.finalUpdateId = finalUpdateId;
        this.bids = bids;
        this.asks = asks;
    }

    @Override
    public String toString() {
        return "OrderBookEvent{" +
                "eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", symbol='" + symbol + '\'' +
                ", firstUpdateId=" + firstUpdateId +
                ", finalUpdateId=" + finalUpdateId +
                ", bids=" + bids +
                ", asks=" + asks +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderBookEvent that = (OrderBookEvent) o;
        return eventTime == that.eventTime && firstUpdateId == that.firstUpdateId && finalUpdateId == that.finalUpdateId && eventType.equals(that.eventType) && symbol.equals(that.symbol) && Objects.equals(bids, that.bids) && Objects.equals(asks, that.asks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventTime, symbol, firstUpdateId, finalUpdateId, bids, asks);
    }
}
