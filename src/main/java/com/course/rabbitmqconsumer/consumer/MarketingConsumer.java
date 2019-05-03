package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class MarketingConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "hr.marketing")
    public void listen(final String msg) throws IOException {
        final Employee readValue = objectMapper.readValue(msg, Employee.class);
        System.out.println("On marketing: " + readValue);
    }
}
