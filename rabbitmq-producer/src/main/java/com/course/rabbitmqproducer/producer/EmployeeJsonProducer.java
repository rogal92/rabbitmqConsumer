package com.course.rabbitmqproducer.producer;

import com.course.rabbitmqproducer.entity.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EmployeeJsonProducer {

    @Autowired
    RabbitTemplate rabbitTemplate;

    private ObjectMapper objectMapper = new ObjectMapper();

    public void sendMessage(Employee employee) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(employee);
        rabbitTemplate.convertAndSend("employeeQueue", json);
    }
}
