package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.java.Log;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Log
public class AccountingConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "hr.accounting")
    public void listen(final String msg) throws IOException {
        final Employee readValue = objectMapper.readValue(msg, Employee.class);
        log.info("On accounting: " + readValue);
    }
}
