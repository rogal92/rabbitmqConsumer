package com.course.rabbitmqconsumer.consumer.errorHandler;

import com.course.rabbitmqconsumer.rabbitmq.RabbitMqHeader;
import com.rabbitmq.client.Channel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.java.Log;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;

import java.io.IOException;
import java.util.Date;

@Log
public class DlxProcessingErrorHandler {
    @NonNull
    @Getter
    private String deadExchangeName;
    @Getter
    private int maxRetryCount = 3;

    public DlxProcessingErrorHandler(String deadExchangeName) {
        if (StringUtils.isEmpty(deadExchangeName))
            throw new IllegalArgumentException("You must define dlx exchange name");

        this.deadExchangeName = deadExchangeName;
    }

    public DlxProcessingErrorHandler(final String deadExchangeName,final int maxRetryCount) {
        this(deadExchangeName);
        setMaxRetryCount(maxRetryCount);
    }

    public boolean handleErrorProcessingMsg(final Message message,final Channel channel) {
        RabbitMqHeader header = new RabbitMqHeader(message.getMessageProperties().getHeaders());

        try {
            if (header.getFailedRetryCount() >= maxRetryCount) {
                logWarn(message, header, "[DEAD]");
                channel.basicPublish(getDeadExchangeName(), message.getMessageProperties().getReceivedRoutingKey(),
                        null, message.getBody());
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            } else {
                logWarn(message, header, "[DEAD]");
                channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
            }
            return true;
        } catch (IOException exc) {
         logWarn(message, header, "[HANDLER-FAILED]");
        }
        return false;
    }

    private void setMaxRetryCount(final int maxRetryCount) throws IllegalArgumentException {
        if (maxRetryCount > 1000) {
            throw new IllegalArgumentException("max retry must between 0-1000");
        }
        this.maxRetryCount = maxRetryCount;
    }

    private void logWarn(Message message, RabbitMqHeader header, String cause) {
        log.warning(String.format("%s Error at: %s on retry: %s for message: %s",cause, new Date(),
                header.getFailedRetryCount(), message));
    }
}
