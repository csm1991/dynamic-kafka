package com.csm.dynamickafka.core;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author caishumin
 * @version 1.0
 * @description
 * @createTime 2025年03月20日 17:51:00
 */
@Component
@Slf4j
public class DynamicKafkaConsumerManager implements SmartLifecycle {

    public DynamicKafkaConsumerManager(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Value("${kafka.consumer.shutdown-timeout:30}")
    private long shutdownTimeout;

    private volatile boolean running = false;

    private final ConsumerFactory<String, String> consumerFactory;
    private final Map<String, ConcurrentMessageListenerContainer<?, ?>> containers = new ConcurrentHashMap<>();

    /**
     * 缓存已解析的@KafkaListener方法信息
     */
    private final Map<String, KafkaListenerMethodWrapper> listenerMethods = new ConcurrentHashMap<>();


    /**
     * listenerMethods元素添加方法
     */
    public void addListenerMethod(String topic, KafkaListenerMethodWrapper listenerMethod) {
        listenerMethods.put(topic, listenerMethod);
    }

    /**
     * 获取listenerMethods
     */
    public Map<String, KafkaListenerMethodWrapper> getListenerMethods() {
        return listenerMethods;
    }

    /**
     * 获取containers
     */
    public Map<String, ConcurrentMessageListenerContainer<?, ?>> getContainers() {
        return containers;
    }

    /**
     * 添加消费者
     */
    public synchronized void addConsumer(String newTopic, String baseTopic, String groupId,
                                         Long tenantId) {
        if (!listenerMethods.containsKey(baseTopic)) {
            //如果找不到父topic，代表原始消费者不在该模块，就忽略
            log.warn("Base topic handler not found: " + baseTopic + ", ignore");
            return;
        }

        if (containers.containsKey(newTopic)) {
            //如果已经存在，就忽略
            log.warn("Duplicate topic handler: " + newTopic + ", ignore");
            return;
        }

        KafkaListenerMethodWrapper wrapper = listenerMethods.get(baseTopic);
        ContainerProperties props = new ContainerProperties(newTopic);
        props.setGroupId(StringUtils.isNotEmpty(groupId) ? groupId : wrapper.getGroupId());
        props.setMessageListener(createBatchListenerAdapter(wrapper));
        props.setShutdownTimeout(shutdownTimeout);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, props);
        container.start();
        containers.put(newTopic, container);
    }

    /**
     * 移除消费者，并组件提供能力但是没有调用，不建议调用
     */
    public void removeConsumer(String topic) {
        ConcurrentMessageListenerContainer<?, ?> container = containers.remove(topic);
        if (container != null) {
            container.stop();
        }
    }

    /**
     * 绑定消费者监听器、ack等信息
     */
    private BatchMessageListener<String, String> createBatchListenerAdapter(
            KafkaListenerMethodWrapper wrapper) {
        return (BatchAcknowledgingConsumerAwareMessageListener<String, String>) (records, acknowledgment, consumer) -> {
            try {
                Object[] args = new Object[wrapper.getMethod().getParameterCount()];
                for (int i = 0; i < args.length; i++) {
                    Class<?> paramType = wrapper.getMethod().getParameterTypes()[i];

                    if (List.class.isAssignableFrom(paramType)) {
                        args[i] = records.stream()
                                .map(r -> new ConsumerRecord<>(r.topic(), r.partition(), r.offset(),
                                        r.key(), r.value()))
                                .collect(Collectors.toList());
                    } else if (Acknowledgment.class.isAssignableFrom(paramType)) {
                        args[i] = new BatchDynamicAcknowledgment(consumer, records);
                    }
                }

                // 反射调用原方法
                wrapper.getMethod().invoke(wrapper.getTargetBean(), args);
            } catch (Exception e) {
                throw new RuntimeException("调用监听器方法失败", e);
            }
        };
    }

    //region spring管理生命周期相关
    @Override
    public void start() {
        this.running = true;
    }

    @Override
    public void stop() {
        if (!this.running) {
            return;
        }
        containers.values().forEach(AbstractMessageListenerContainer::stop);
        containers.clear();
        this.running = false;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE; // 确保最后关闭
    }
    //endregion
}


