package com.course.rabbitmqconsumer.consumer;

import com.course.rabbitmqconsumer.consumer.errorHandler.DlxProcessingErrorHandler;
import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.java.Log;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

import java.io.IOException;
import java.util.Arrays;

@Log
public class RetryImageConsumer {

    private static final String DEAD_EXCHANGE_NAME = "x.guideline.dead";
    private DlxProcessingErrorHandler dlxProcessingErrorHandler;

    private ObjectMapper objectMapper;

    public RetryImageConsumer() {
        this.objectMapper = new ObjectMapper();
        this.dlxProcessingErrorHandler = new DlxProcessingErrorHandler(DEAD_EXCHANGE_NAME);
    }

    @RabbitListener(queues = "q.guideline.image.work")
    public void listen(Message message, Channel channel)
            throws InterruptedException, JsonParseException, JsonMappingException, IOException {
        try {
            Picture p = objectMapper.readValue(message.getBody(), Picture.class);
            if (p.getSize() > 9000) {
                throw new IOException("Size too large");
            } else {
                log.info("Creating thumbnail & publishing : " + p);
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            }
        } catch (IOException e) {
            log.warning(String.format("Error processing message : %s : %s", new String(message.getBody()), e.getMessage()));
            dlxProcessingErrorHandler.handleErrorProcessingMsg(message, channel);
        }
    }
}
