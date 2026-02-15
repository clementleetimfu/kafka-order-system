package io.clementleetimfu.ordervalidation.consumer;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import io.clementleetimfu.ordercommon.constants.GroupConstants;
import io.clementleetimfu.ordercommon.constants.StatusConstants;
import io.clementleetimfu.ordercommon.constants.TopicConstants;
import io.clementleetimfu.ordercommon.event.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class OrderValidationConsumer {

    @Autowired
    private KafkaTemplate<String, OrderValidationResult> kafkaTemplate;

    /**
     * Topics created automatically:
     * 1. order-placed                        (main topic)
     * 2. order-placed-retry-0                (1st retry - 2s delay)
     * 3. order-placed-retry-1                (2nd retry - 4s delay)
     * 4. order-placed-retry-2                (3rd retry - 8s delay)
     * 5. order-placed-dlt                    (dead letter topic)
     */
    @RetryableTopic(
            attempts = "4",  // 1 original + 3 retries
            backoff = @Backoff(
                    delay = 2000,      // initial delay: 2 seconds
                    multiplier = 2.0,  // exponential: 2s -> 4s -> 8s
                    maxDelay = 16000   // Cap at 16 seconds
            ),
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = {Exception.class}
    )
    @KafkaListener(topics = TopicConstants.ORDER_PLACED, groupId = GroupConstants.VALIDATION)
    public void onOrderPlaced(ConsumerRecord<String, OrderPlacedEvent> consumerRecord, Acknowledgment acknowledgment) {

        String attempt = determineAttempt(consumerRecord.topic());

        OrderPlacedEvent orderPlacedEvent = consumerRecord.value();

        try {
            List<String> failureReasons = validate(orderPlacedEvent);

            if (failureReasons.isEmpty()) {
                OrderConfirmedEvent orderConfirmedEvent = OrderConfirmedEvent.builder()
                        .orderId(orderPlacedEvent.getOrderId())
                        .customerId(orderPlacedEvent.getCustomerId())
                        .email(orderPlacedEvent.getEmail())
                        .region(orderPlacedEvent.getRegion())
                        .items(orderPlacedEvent.getItems())
                        .totalAmount(orderPlacedEvent.getTotalAmount())
                        .status(StatusConstants.CONFIRMED)
                        .confirmedAt(Instant.now())
                        .validatedBy(GroupConstants.VALIDATION)
                        .build();

                CompletableFuture<SendResult<String, OrderValidationResult>> completableFuture =
                        kafkaTemplate.send(TopicConstants.ORDER_CONFIRMED, orderConfirmedEvent);
                attachCallbacks(completableFuture, orderConfirmedEvent, acknowledgment);

            } else {

                OrderFailedEvent orderFailedEvent = OrderFailedEvent.builder()
                        .orderId(orderPlacedEvent.getOrderId())
                        .customerId(orderPlacedEvent.getCustomerId())
                        .email(orderPlacedEvent.getEmail())
                        .region(orderPlacedEvent.getRegion())
                        .status(StatusConstants.FAILED)
                        .failureReasons(failureReasons)
                        .failedAt(Instant.now())
                        .build();

                CompletableFuture<SendResult<String, OrderValidationResult>> completableFuture =
                        kafkaTemplate.send(TopicConstants.ORDER_FAILED, orderFailedEvent);
                attachCallbacks(completableFuture, orderFailedEvent, acknowledgment);

            }

        } catch (Exception e) {
            log.error("Order validation failed; orderId={}, attempt={}, error={}",
                    orderPlacedEvent.getOrderId(), attempt, e.getMessage(), e);
            throw new RuntimeException("Order validation failed: ", e);
        }
    }

    @DltHandler
    public void handleDlt(ConsumerRecord<String, OrderPlacedEvent> consumerRecord, Acknowledgment acknowledgment) {
        String attempt = determineAttempt(consumerRecord.topic());
        try {
            // Possible optimization: webhook to communication tools/ database logging
            log.error("Successfully handle DLT event; attempt:{}, topic:{}, order placed event:{}",
                    attempt, consumerRecord.topic(), consumerRecord.value());
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to handle DLT event; attempt:{}, topic:{}, order placed event:{}",
                    attempt, consumerRecord.topic(), consumerRecord.value(), e);
        }
    }

    private List<String> validate(OrderPlacedEvent orderPlacedEvent) {
        List<String> failureReasons = new ArrayList<>();

        if (StrUtil.isBlank(orderPlacedEvent.getCustomerId())) {
            failureReasons.add("Customer Id is required");
        }

        if (StrUtil.isBlank(orderPlacedEvent.getRegion())) {
            failureReasons.add("Region is required");
        }

        if (StrUtil.isBlank(orderPlacedEvent.getPriority())) {
            failureReasons.add("Priority is required");
        }

        if (StrUtil.isBlank(orderPlacedEvent.getEmail())) {
            failureReasons.add("Email is required");
        }

        if (CollectionUtil.isEmpty(orderPlacedEvent.getItems())) {
            failureReasons.add("Items list is required");
        } else {
            for (OrderItem item : orderPlacedEvent.getItems()) {
                if (StrUtil.isBlank(item.getProductId())) {
                    failureReasons.add("Product ID is required");
                }
                if (StrUtil.isBlank(item.getProductName())) {
                    failureReasons.add("Product name is required");
                }
                if (null == item.getQuantity()) {
                    failureReasons.add("Item quantity is required");
                }
                if (null == item.getPrice()) {
                    failureReasons.add("Item price is required");
                }
                if (null != item.getQuantity() && item.getQuantity() <= 0) {
                    failureReasons.add("Item quantity is invalid");
                }
                if (null != item.getPrice() && item.getPrice().compareTo(BigDecimal.ZERO) <= 0) {
                    failureReasons.add("Item price is invalid");
                }
            }
        }

        return failureReasons;
    }

    private String determineAttempt(String topic) {
        if (topic.endsWith("-retry-0")) {
            return "RETRY 1/3 (from retry-0 topic)";
        }
        if (topic.endsWith("-retry-1")) {
            return "RETRY 2/3 (from retry-1 topic)";
        }
        if (topic.endsWith("-retry-2")) {
            return "RETRY 3/3 (from retry-2 topic)";
        }
        if (topic.endsWith("-dlt")) {
            return "DEAD LETTER";
        }
        return "ORIGINAL (1/4)";
    }

    private void attachCallbacks(CompletableFuture<SendResult<String, OrderValidationResult>> completableFuture,
                                 OrderValidationResult orderValidationResult,
                                 Acknowledgment acknowledgment) {
        completableFuture.thenAccept(result -> {
            log.info("Order forward successfully: topic={}, partition={}, offset={}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            acknowledgment.acknowledge();
        }).exceptionally(e -> {
            String orderId = "";
            String type = "";
            if (orderValidationResult instanceof OrderConfirmedEvent confirmedEvent) {
                orderId = confirmedEvent.getOrderId();
                type = confirmedEvent.getClass().getSimpleName();
            } else if (orderValidationResult instanceof OrderFailedEvent failedEvent) {
                orderId = failedEvent.getOrderId();
                type = failedEvent.getClass().getSimpleName();
            }
            log.error("Failed to forward {}; order ID: {}", type, orderId, e);
            return null;
        });
    }

}