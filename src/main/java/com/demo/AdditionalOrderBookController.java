package com.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/startQueueForBook")
public class AdditionalOrderBookController {

    @Autowired
    private DatabaseConfig databaseConfig;


    @Autowired
    private MessageSenderService messageSenderService;

    @GetMapping
    public void startWritingOrderBook(@RequestParam long startTime,
                                      @RequestParam long endTime,
                                      @RequestParam int intervalMinutes,
                                      @RequestParam String eventSymbol,
                                      @RequestParam String name_queue) {
            OrderBookManager orderBookManager = new OrderBookManager();
            AppConfig appConfig = new AppConfig();
            appConfig.setEventSymbol(eventSymbol);
            orderBookManager.createNewInstance(databaseConfig, appConfig);
            orderBookManager.setMessageSenderService(messageSenderService);
        try {
            orderBookManager.startWritingTemporaryBook(startTime, endTime, intervalMinutes, name_queue);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
