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

@Log
public class RetryVectorConsumer {

    private static final String DEAD_EXCHANGE_NAME = "x.guideline.dead";

    private DlxProcessingErrorHandler dlxProcessingErrorHandler;

    private ObjectMapper objectMapper;

    public RetryVectorConsumer() {
        this.objectMapper = new ObjectMapper();
        this.dlxProcessingErrorHandler = new DlxProcessingErrorHandler(DEAD_EXCHANGE_NAME);
    }

    @RabbitListener(queues = "q.guideline.vector.work")
    public void listen(final Message message,final Channel channel)
            throws InterruptedException, JsonParseException, JsonMappingException, IOException {
        try {
            Picture p = objectMapper.readValue(message.getBody(), Picture.class);
            validateSizeAndAck(message, channel, p);
        } catch (IOException e) {
            log.warning("Error processing message : " + new String(message.getBody()) + " : " + e.getMessage());
            dlxProcessingErrorHandler.handleErrorProcessingMsg(message, channel);
        }
    }

    private void validateSizeAndAck(final Message message, final Channel channel, final Picture p) throws IOException {
        if (p.getSize() > 9000) {
            throw new IOException("Size too large");
        } else {
            log.info("Convert to image, creating thumbnail, & publishing : " + p);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        }
    }
}
