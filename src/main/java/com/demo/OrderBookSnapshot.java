package com.demo;

import java.util.List;

public class OrderBookSnapshot {
    private long lastUpdateId;
    private List<OrderBookEvent.PriceQuantityPair> bids;
    private List<OrderBookEvent.PriceQuantityPair> asks;

    public long getLastUpdateId() {
        return lastUpdateId;
    }

    public List<OrderBookEvent.PriceQuantityPair> getBids() {
        return bids;
    }

    public List<OrderBookEvent.PriceQuantityPair> getAsks() {
        return asks;
    }

    public void setLastUpdateId(long lastUpdateId) {
        this.lastUpdateId = lastUpdateId;
    }

    public void setBids(List<OrderBookEvent.PriceQuantityPair> bids) {
        this.bids = bids;
    }

    public void setAsks(List<OrderBookEvent.PriceQuantityPair> asks) {
        this.asks = asks;
    }
}
