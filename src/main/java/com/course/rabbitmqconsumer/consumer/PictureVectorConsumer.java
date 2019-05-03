package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.java.Log;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Log
public class PictureVectorConsumer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "picture.vector")
    public void listen(String message) throws IOException {
        final Picture picture = objectMapper.readValue(message, Picture.class);
        log.info("Converting to image, creating thumbail & publishing: " + picture);
    }
}
