package com.csm.dynamickafka.core;

import java.lang.reflect.Method;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author caishumin
 * @version 1.0
 * @description
 * @createTime 2025年03月21日 14:06:00
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

