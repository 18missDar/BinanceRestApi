package com.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
    public String getOrderBook(@RequestParam long currentTime) {
        OrderBookSnapshot orderBookSnapshot = null;
        try {
            orderBookSnapshot = orderBookManager.collectData(currentTime);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String resultJson = "{\n" +
                "\"lastUpdateId\":" + orderBookSnapshot.getLastUpdateId() + "," +
               "\"bids\": " + orderBookSnapshot.getBids() +
                ", \"asks\": " + orderBookSnapshot.getAsks() + "}";
        return resultJson;
    }
}
