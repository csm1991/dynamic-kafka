package com.csm.dynamickafka.core;

import java.lang.reflect.Method;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Kafka消费者监听信息缓存实体
 *
 * @author Simon Cai
 * @version 1.0
 * @since 2025-05-10
 */
@Getter
@AllArgsConstructor
public class KafkaListenerMethodWrapper {
    private Object targetBean;
    private Method method;
    private KafkaListener annotation;

    public String getGroupId() {
        return annotation.groupId().isEmpty() ?
                "dynamic-group-" + UUID.randomUUID() :
                annotation.groupId();
    }
}

