package com.demo;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;

@Service
public class MessageSenderService {

    private final RabbitTemplate rabbitTemplate;

    private final RabbitAdmin rabbitAdmin;

    @Autowired
    public MessageSenderService(RabbitTemplate rabbitTemplate, RabbitAdmin rabbitAdmin) {
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitAdmin = rabbitAdmin;
    }

    public void sendMessage(String queueName, String message) {
        // Send the message to the queue
        rabbitTemplate.convertAndSend(queueName, message);
    }

    public void createQueue(String queueName) {
        // Generate a dynamic queue name based on the current date
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        queueName = queueName + "_" + dateFormat.format(new Date());
        Queue queue = new Queue(queueName);
        rabbitAdmin.declareQueue(queue);
    }
}