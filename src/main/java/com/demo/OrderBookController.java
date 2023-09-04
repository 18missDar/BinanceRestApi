package com.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/getOrderBook")
public class OrderBookController {

    @Autowired
    private DatabaseConfig databaseConfig;

    @GetMapping
    public String getOrderBook(@RequestParam long currentTime, @RequestParam String eventSymbol) {
        OrderBookSnapshot orderBookSnapshot = null;
        try {
            OrderBookManager orderBookManager = new OrderBookManager();
            AppConfig appConfig = new AppConfig();
            appConfig.setEventSymbol(eventSymbol);
            orderBookManager.createNewInstance(databaseConfig, appConfig);
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
