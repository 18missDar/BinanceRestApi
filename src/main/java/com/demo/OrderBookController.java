package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/getOrderBook")
public class OrderBookController {

    private final OrderBookManager orderBookManager;

    public OrderBookController(OrderBookManager orderBookManager) {
        this.orderBookManager = orderBookManager;
    }

    @GetMapping
    public String getOrderBook() {
        List<OrderBookEvent.PriceQuantityPair> bids = orderBookManager.getBidsActual();
        List<OrderBookEvent.PriceQuantityPair> asks = orderBookManager.getAsksActual();
        String resultJson = "{\n" +
                "\"lastUpdateId\":" + orderBookManager.getLastUpdateId() + "," +
               "\"bids\": " + bids.toString() +
                ", \"asks\": " + asks.toString() + "}";
        return resultJson;
    }
}
