package com.course.rabbitmqconsumer.rabbitmq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * Represents RabbitMQ Header. Tested on RabbitMQ 3.7.x.
 */

@Getter
@Setter
public class RabbitMqHeader {

    private static final String KEYWORD_QUEUE_WAIT = "wait";
    private List<RabbitMqHeaderXDeath> xDeaths = new ArrayList<>(2);
    private String xFirstDeathExchange = StringUtils.EMPTY;
    private String xFirstDeathQueue = StringUtils.EMPTY;
    private String xFirstDeathReason = StringUtils.EMPTY;

    @SuppressWarnings("unchecked")
    public RabbitMqHeader(Map<String, Object> headers) {
        if (Optional.ofNullable(headers).isPresent()) {
            Optional<Object> xFirstDeathExchange = Optional.ofNullable(headers.get("x-first-death-exchange"));
            Optional<Object> xFirstDeathQueue = Optional.ofNullable(headers.get("x-first-death-queue"));
            Optional<Object> xFirstDeathReason = Optional.ofNullable(headers.get("x-first-death-reason"));

            xFirstDeathExchange.ifPresent(s -> this.setXFirstDeathExchange(s.toString()));
            xFirstDeathQueue.ifPresent(s -> this.setXFirstDeathQueue(s.toString()));
            xFirstDeathReason.ifPresent(s -> this.setXFirstDeathReason(s.toString()));

            List<Map<String, Object>> xDeathHeaders = (List<Map<String, Object>>) headers.get("x-death");

            if (Optional.ofNullable(xDeathHeaders).isPresent()) {
                for (Map<String, Object> x : xDeathHeaders) {
                    RabbitMqHeaderXDeath hdrDeath = new RabbitMqHeaderXDeath();
                    Optional<Object> reason = Optional.ofNullable(x.get("reason"));
                    Optional<Object> count = Optional.ofNullable(x.get("count"));
                    Optional<Object> exchange = Optional.ofNullable(x.get("exchange"));
                    Optional<Object> queue = Optional.ofNullable(x.get("queue"));
                    Optional<Object> routingKeys = Optional.ofNullable(x.get("routing-keys"));
                    Optional<Object> time = Optional.ofNullable(x.get("time"));

                    reason.ifPresent(s -> hdrDeath.setReason(s.toString()));
                    count.ifPresent(s -> hdrDeath.setCount(Integer.parseInt(s.toString())));
                    exchange.ifPresent(s -> hdrDeath.setExchange(s.toString()));
                    queue.ifPresent(s -> hdrDeath.setQueue(s.toString()));
                    routingKeys.ifPresent(r -> {
                        List<String> listR = (List<String>) r;
                        hdrDeath.setRoutingKeys(listR);
                    });
                    time.ifPresent(d -> hdrDeath.setTime((Date) d));

                    xDeaths.add(hdrDeath);
                }
            }
        }
    }

    public int getFailedRetryCount() {
        // get from queue "wait"
        for (RabbitMqHeaderXDeath xDeath : xDeaths) {
            if (xDeath.getExchange().toLowerCase().endsWith(KEYWORD_QUEUE_WAIT)
                    && xDeath.getQueue().toLowerCase().endsWith(KEYWORD_QUEUE_WAIT)) {
                return xDeath.getCount();
            }
        }
        return 0;
    }
}
