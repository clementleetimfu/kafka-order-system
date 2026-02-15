package io.clementleetimfu.orderapi.producer;

import io.clementleetimfu.ordercommon.constants.TopicConstants;
import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

    public CompletableFuture<SendResult<String, OrderPlacedEvent>> sendOrder(OrderPlacedEvent orderPlacedEvent) {
        CompletableFuture<SendResult<String, OrderPlacedEvent>> completableFuture =
                kafkaTemplate.send(TopicConstants.ORDER_PLACED, orderPlacedEvent.getOrderId(), orderPlacedEvent);

        attachCallbacks(completableFuture, orderPlacedEvent);
        return completableFuture;
    }

    public CompletableFuture<SendResult<String, OrderPlacedEvent>> sendOrderDefault(OrderPlacedEvent orderPlacedEvent) {
        CompletableFuture<SendResult<String, OrderPlacedEvent>> completableFuture =
                kafkaTemplate.sendDefault(orderPlacedEvent.getOrderId(), orderPlacedEvent);

        attachCallbacks(completableFuture, orderPlacedEvent);
        return completableFuture;
    }

    private void attachCallbacks(CompletableFuture<SendResult<String, OrderPlacedEvent>> completableFuture, OrderPlacedEvent orderPlacedEvent) {
        completableFuture.thenAccept(result -> {
            log.debug("Order sent successfully: topic={}, partition={}, offset={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        }).exceptionally(e -> {
            log.error("Failed to send order {}", orderPlacedEvent.getOrderId(), e);
            return null;
        });
    }
}
