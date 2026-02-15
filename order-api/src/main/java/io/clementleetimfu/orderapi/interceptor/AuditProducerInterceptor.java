package io.clementleetimfu.orderapi.interceptor;

import io.clementleetimfu.ordercommon.constants.HeaderConstants;
import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
public class AuditProducerInterceptor implements ProducerInterceptor<String, OrderPlacedEvent> {

    private static final String SOURCE = "order-api";

    @Override
    public ProducerRecord<String, OrderPlacedEvent> onSend(ProducerRecord<String, OrderPlacedEvent> record) {
        record.headers().add(HeaderConstants.CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HeaderConstants.TIMESTAMP, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(HeaderConstants.SOURCE, SOURCE.getBytes(StandardCharsets.UTF_8));
        log.debug("Added audit header for orderId:{}", record.key());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("Failed to acknowledge", exception);
        } else {
            log.debug("Successfully acknowledged, topic: {}, offset: {}, partition: {}, timestamp: {}",
                    metadata.topic(), metadata.offset(), metadata.partition(), metadata.timestamp());
        }
    }

    @Override
    public void close() {
        log.debug("AuditProducerInterceptor closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.debug("AuditProducerInterceptor configured");
    }
}