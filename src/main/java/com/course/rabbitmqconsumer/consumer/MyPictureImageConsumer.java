package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Log
public class MyPictureImageConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @RabbitListener(queues = "mypicture.image")
    public void listen(final Message message,final Channel channel) throws IOException {
        final Picture picture = objectMapper.readValue(message.getBody(), Picture.class);
        rejectIfBiggerThan99(message, channel, picture);
        log.info("Creating thumbail & publishing: " + picture);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    private void rejectIfBiggerThan99(Message message, Channel channel, Picture picture) throws IOException {
        if (picture.getSize() > 99)
            channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
    }

}
