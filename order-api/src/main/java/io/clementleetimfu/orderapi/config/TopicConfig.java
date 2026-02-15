package io.clementleetimfu.orderapi.config;

import io.clementleetimfu.ordercommon.constants.TopicConstants;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopicConfig {

    @Bean
    public NewTopic orderPlacedTopic() {
        return new NewTopic(TopicConstants.ORDER_PLACED, TopicConstants.PARTITIONS, (short) TopicConstants.REPLICAS);
    }

    @Bean
    public NewTopic orderConfirmedTopic() {
        return new NewTopic(TopicConstants.ORDER_CONFIRMED, TopicConstants.PARTITIONS, (short) TopicConstants.REPLICAS);
    }

    @Bean
    public NewTopic orderFailedTopic() {
        return new NewTopic(TopicConstants.ORDER_FAILED, TopicConstants.PARTITIONS, (short) TopicConstants.REPLICAS);
    }
}
