package com.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/getOrderTemporaryBook")
public class OrderTemporaryBookController {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @GetMapping
    public String getTemporaryOrderBook(@RequestParam String name_queue) {
        OrderBookSnapshot orderBookSnapshot = null;
        OrderBookManager orderBookManager = new OrderBookManager();
        orderBookManager.setRabbitTemplate(rabbitTemplate);
        return orderBookManager.getTemporaryOrderBook(name_queue);
    }
}
