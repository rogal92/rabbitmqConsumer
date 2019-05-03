package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.java.Log;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Log
public class PictureFilterConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "picture.filter")
    public void listen(String message) throws IOException {
        final Picture picture = objectMapper.readValue(message, Picture.class);
        log.info("Applying filter: " + picture);
    }
}
