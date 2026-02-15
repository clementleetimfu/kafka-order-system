package io.clementleetimfu.orderapi.partitioner;

import io.clementleetimfu.ordercommon.constants.RegionConstants;
import io.clementleetimfu.ordercommon.event.OrderPlacedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

@Slf4j
public class RegionPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> availablePartitionsForTopic = cluster.availablePartitionsForTopic(topic);
        int numPartitions = availablePartitionsForTopic.size();

        if (value == null) {
            return fallbackPartition(keyBytes, numPartitions);
        }

        String region = extractRegion(value);

        if (region == null) {
            return fallbackPartition(keyBytes, numPartitions);
        }

        return switch (region) {
            case RegionConstants.ASIA -> 0;
            case RegionConstants.EUROPE -> 1;
            case RegionConstants.AMERICA -> 2;
            default -> fallbackPartition(keyBytes, numPartitions);
        };
    }

    @Override
    public void close() {
        log.debug("RegionPartitioner closed");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.debug("RegionPartitioner configured");
    }

    private String extractRegion(Object value) {
        if (value instanceof OrderPlacedEvent orderPlacedEvent) {
            return orderPlacedEvent.getRegion();
        }
        return null;
    }

    private int fallbackPartition(byte[] keyBytes, int numPartitions) {
        if (keyBytes == null) {
            return 0;
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }
}