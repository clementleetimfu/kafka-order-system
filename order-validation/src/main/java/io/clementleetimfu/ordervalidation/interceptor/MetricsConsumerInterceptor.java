package io.clementleetimfu.ordervalidation.interceptor;

import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MetricsConsumerInterceptor implements ConsumerInterceptor<String, OrderPlacedEvent> {

    private AtomicLong totalMessagesConsumed = new AtomicLong(0);
    private AtomicLong totalBytesConsumed = new AtomicLong(0);
    private AtomicLong totalCommits = new AtomicLong(0);

    @Override
    public ConsumerRecords<String, OrderPlacedEvent> onConsume(ConsumerRecords<String, OrderPlacedEvent> records) {

        records.forEach(record -> {
            if (isOriginalTopic(record.topic())) {
                totalMessagesConsumed.incrementAndGet();
                totalBytesConsumed.addAndGet(record.serializedValueSize());
                log.info("Total message consumed: {}", totalMessagesConsumed.get());
                log.info("Total bytes consumed: {}", totalBytesConsumed.get());
            }
        });

        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (null == offsets || offsets.isEmpty()) {
            return;
        }
        totalCommits.incrementAndGet();
        offsets.forEach((topicPartition, offsetAndMetadata) ->
                log.info("Commited offset; topic: {}, partition: {}, offset: {}",
                        topicPartition.topic(),
                        topicPartition.partition(),
                        offsetAndMetadata.offset())
        );
        log.info("Total commits: {}", totalCommits.get());
    }

    @Override
    public void close() {
        log.debug("MetricsConsumerInterceptor closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.debug("MetricsConsumerInterceptor configured");
    }

    private boolean isOriginalTopic(String topic) {
        return !topic.contains("-retry-") && !topic.endsWith("-dlt");
    }
}