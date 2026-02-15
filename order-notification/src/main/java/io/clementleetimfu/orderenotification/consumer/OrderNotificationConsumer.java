package io.clementleetimfu.orderenotification.consumer;

import io.clementleetimfu.ordercommon.constants.GroupConstants;
import io.clementleetimfu.ordercommon.constants.TopicConstants;
import io.clementleetimfu.ordercommon.event.OrderConfirmedEvent;
import io.clementleetimfu.ordercommon.event.OrderFailedEvent;
import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import io.clementleetimfu.orderenotification.service.MailgunService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderNotificationConsumer {

    @Autowired
    private MailgunService emailService;

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 16000),
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = {Exception.class})
    @KafkaListener(
            groupId = GroupConstants.EMAIL,
            topicPartitions = {
                    @TopicPartition(
                            topic = TopicConstants.ORDER_CONFIRMED,
                            partitions = {"0", "1", "2"})
            }, containerFactory = "confirmedKafkaListenerContainerFactory")
    public void onOrderConfirmed(ConsumerRecord<String, OrderConfirmedEvent> consumerRecord, Acknowledgment ack) {
        String attempt = determineAttempt(consumerRecord.topic());
        try {
            emailService.sendOrderConfirmedEmail(consumerRecord.value());
            ack.acknowledge();

            log.info("Order confirmed email sent successfully; attempt: {}, topic:{}, order confirmed event:{}",
                    attempt, consumerRecord.topic(), consumerRecord.value());

        } catch (Exception e) {
            log.error("Order confirmed email failed to send; attempt: {}, topic:{}, order confirmed event:{}",
                    attempt, consumerRecord.topic(), consumerRecord.value(), e);
            throw new RuntimeException("Order confirmed email failed to send: ", e);
        }
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 16000),
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = {Exception.class})
    @KafkaListener(
            groupId = GroupConstants.EMAIL,
            topicPartitions = {
                    @TopicPartition(
                            topic = TopicConstants.ORDER_FAILED,
                            partitions = {"0", "1", "2"})
            }, containerFactory = "failedKafkaListenerContainerFactory")
    public void onOrderFailed(ConsumerRecord<String, OrderFailedEvent> consumerRecord, Acknowledgment ack) {
        String attempt = determineAttempt(consumerRecord.topic());
        try {
            emailService.sendOrderFailedEmail(consumerRecord.value());
            ack.acknowledge();
            log.info("Order failed email sent successfully; attempt: {}, topic:{}, order failed event:{}",
                    attempt, consumerRecord.topic(), consumerRecord.value());
        } catch (Exception e) {
            log.error("Order failed email failed to send; attempt: {}, topic:{}, order failed event:{}",
                    attempt, consumerRecord.topic(), consumerRecord.value(), e);
            throw new RuntimeException("Order failed email failed to send: ", e);
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
}
