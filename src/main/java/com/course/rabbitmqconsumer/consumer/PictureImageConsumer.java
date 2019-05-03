package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.java.Log;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Log
public class PictureImageConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "pictures.image")
    public void listen(String message) throws IOException {
        final Picture picture = objectMapper.readValue(message, Picture.class);
        log.info("Creating thumbail & publishing: " + picture);
    }
}
