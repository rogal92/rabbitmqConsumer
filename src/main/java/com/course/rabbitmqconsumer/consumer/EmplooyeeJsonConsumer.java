package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.consumer.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class EmplooyeeJsonConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "employeeQueue")
    public void listen(String msg) throws IOException {
        Employee readValue = objectMapper.readValue(msg, Employee.class);
        System.out.println(readValue);
    }
}
