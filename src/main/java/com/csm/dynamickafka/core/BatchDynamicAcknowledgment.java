package com.csm.dynamickafka.core;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;

/**
 * 批量模式的ACK
 *
 * @author Simon Cai
 * @version 1.0
 * @since 2025-05-10
 */
public class BatchDynamicAcknowledgment implements Acknowledgment {

    private final Consumer<?, ?> consumer;
    private final List<ConsumerRecord<String, String>> records;

    public BatchDynamicAcknowledgment(Consumer<?, ?> consumer, List<ConsumerRecord<String, String>> records) {
        this.consumer = consumer;
        this.records = records;
    }

    @Override
    public void acknowledge() {
        // 计算每个分区的最大偏移量
        Map<TopicPartition, OffsetAndMetadata> offsets = records.stream()
                .collect(Collectors.toMap(
                        r -> new TopicPartition(r.topic(), r.partition()),
                        r -> new OffsetAndMetadata(r.offset() + 1),
                        // 同一分区取最大offset
                        (existing, replacement) ->
                                existing.offset() > replacement.offset() ? existing : replacement
                ));

        try {
            consumer.commitSync(offsets);
        } catch (CommitFailedException e) {
            throw new RuntimeException("提交偏移量失败", e);
        }
    }
}

