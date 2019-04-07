package com.course.rabbitmqproducer.producer;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class FixedRateProducer {

    private int i;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Scheduled(fixedRate = 500)
    public void sendMsg() {
        i++;
//        System.out.println("Msg number: " + i);
        rabbitTemplate.convertAndSend("course.fixedrate", "Fixed rate: " + i);
    }
}
