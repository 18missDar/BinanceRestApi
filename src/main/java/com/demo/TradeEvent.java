package com.demo;

import java.util.Objects;

public class TradeEvent {
    private String eventType;
    private long eventTime;
    private String symbol;
    private long tradeId;
    private String price;
    private String quantity;
    private long buyerOrderId;
    private long sellerOrderId;
    private long tradeTime;
    private boolean isBuyerMarketMaker;

    public TradeEvent(String eventType, long eventTime, String symbol, long tradeId, String price, String quantity,
                      long buyerOrderId, long sellerOrderId, long tradeTime, boolean isBuyerMarketMaker) {
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.symbol = symbol;
        this.tradeId = tradeId;
        this.price = price;
        this.quantity = quantity;
        this.buyerOrderId = buyerOrderId;
        this.sellerOrderId = sellerOrderId;
        this.tradeTime = tradeTime;
        this.isBuyerMarketMaker = isBuyerMarketMaker;
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

    public long getTradeId() {
        return tradeId;
    }

    public void setTradeId(long tradeId) {
        this.tradeId = tradeId;
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

    public long getBuyerOrderId() {
        return buyerOrderId;
    }

    public void setBuyerOrderId(long buyerOrderId) {
        this.buyerOrderId = buyerOrderId;
    }

    public long getSellerOrderId() {
        return sellerOrderId;
    }

    public void setSellerOrderId(long sellerOrderId) {
        this.sellerOrderId = sellerOrderId;
    }

    public long getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(long tradeTime) {
        this.tradeTime = tradeTime;
    }

    public boolean isBuyerMarketMaker() {
        return isBuyerMarketMaker;
    }

    public void setBuyerMarketMaker(boolean buyerMarketMaker) {
        isBuyerMarketMaker = buyerMarketMaker;
    }

    // Example implementation of the toString() method to print the TradeEvent object
    @Override
    public String toString() {
        return "{" +
                "\"e\":\"" + eventType + "\"," +
                "\"E\":" + eventTime + "," +
                "\"s\":\"" + symbol + "\"," +
                "\"t\":" + tradeId + "," +
                "\"p\":\"" + price + "\"," +
                "\"q\":\"" + quantity + "\"," +
                "\"b\":" + buyerOrderId + "," +
                "\"a\":" + sellerOrderId + "," +
                "\"T\":" + tradeTime + "," +
                "\"m\":" + isBuyerMarketMaker + "," +
                "\"M\":" + isBuyerMarketMaker +
                "}";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TradeEvent that = (TradeEvent) o;
        return eventTime == that.eventTime && tradeId == that.tradeId && buyerOrderId == that.buyerOrderId && sellerOrderId == that.sellerOrderId && tradeTime == that.tradeTime && isBuyerMarketMaker == that.isBuyerMarketMaker && eventType.equals(that.eventType) && symbol.equals(that.symbol) && Objects.equals(price, that.price) && Objects.equals(quantity, that.quantity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventTime, symbol, tradeId, price, quantity, buyerOrderId, sellerOrderId, tradeTime, isBuyerMarketMaker);
    }
}
