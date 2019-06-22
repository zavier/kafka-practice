package com.zavier.kafka.producer.partition;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义分区起器，用于发送前执行发送的分区位置
 * 默认的分区器为 DefaultPartitioner
 */
@Slf4j
public class DemoPartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        log.info("partitionInfos:{}", partitionInfos);
        final int numPartitions = partitionInfos.size();
        if (keyBytes == null) {
            return counter.getAndIncrement() % numPartitions;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes) % numPartitions);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
