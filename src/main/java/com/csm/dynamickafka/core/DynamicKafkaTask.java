package com.csm.dynamickafka.core;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.csm.dynamickafka.entity.ConfigDynamicKafka;
import com.csm.dynamickafka.service.IConfigDynamicKafkaService;

import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @author caishumin
 * @version 1.0
 * @description
 * @createTime 2025年03月24日 11:20:00
 */
@Component
@Slf4j
public class DynamicKafkaTask {

    private final IConfigDynamicKafkaService iConfigDynamicKafkaService;

    private final DynamicKafkaConsumerManager dynamicKafkaConsumerManager;

    private final DynamicKafkaProducerManager dynamicKafkaProducerManager;

    public DynamicKafkaTask(IConfigDynamicKafkaService iConfigDynamicKafkaService, DynamicKafkaConsumerManager dynamicKafkaConsumerManager, DynamicKafkaProducerManager dynamicKafkaProducerManager) {
        this.iConfigDynamicKafkaService = iConfigDynamicKafkaService;
        this.dynamicKafkaConsumerManager = dynamicKafkaConsumerManager;
        this.dynamicKafkaProducerManager = dynamicKafkaProducerManager;
    }

    /**
     * 本地JOB，不要修改成XXL-JOB，因为所有信息都在内容中，需要保证每个应用都正确加载
     */
    @Scheduled(cron = "0 0/1 * * * ?")
    public void doUpdate() {
        try {
            DynamicKafkaInit.awaitInitialization();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("等待Kafka初始化时发生中断异常", e);
            return;
        }

        //查询数据库中有效的配置
        LambdaQueryWrapper<ConfigDynamicKafka> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ConfigDynamicKafka::getDeleteFlag, 0);
        List<ConfigDynamicKafka> configDynamicKafkaList = iConfigDynamicKafkaService.list(
                queryWrapper);
        if (CollectionUtils.isEmpty(configDynamicKafkaList)) {
            //不存在配置，则直接返回
            return;
        }

        //添加转发配置到内存中
        for (ConfigDynamicKafka configDynamicKafka : configDynamicKafkaList) {
            dynamicKafkaProducerManager.addTopicMap(configDynamicKafka.getOriginalTopic(),
                    configDynamicKafka.getNewTopic(), configDynamicKafka.getTenantId());
        }

        //获取内存中已经注册的动态消费者组
        Map<String, ConcurrentMessageListenerContainer<?, ?>> existContainers = dynamicKafkaConsumerManager.getContainers();
        Map<String, KafkaListenerMethodWrapper> listenerMethods = dynamicKafkaConsumerManager.getListenerMethods();

        //遍历configDynamicKafkaList，与existContainers比较，如果存在，则忽略，如果不存在，则添加
        for (ConfigDynamicKafka configDynamicKafka : configDynamicKafkaList) {
            String newTopic = configDynamicKafka.getNewTopic();
            String baseTopic = configDynamicKafka.getOriginalTopic();
            String groupId = configDynamicKafka.getNewGroupId();
            Long tenantId = configDynamicKafka.getTenantId();
            if (!existContainers.containsKey(newTopic) && listenerMethods.containsKey(baseTopic)) {
                log.info("添加动态消费者组，newTopic：{}，baseTopic：{}，groupId：{}", newTopic,
                        baseTopic, groupId);
                dynamicKafkaConsumerManager.addConsumer(newTopic, baseTopic, groupId, tenantId);
            }
        }

        //获取不同租户的baseTopic对应的newTopic缓存
        Map<String, String> baseTopicToNewTopicMap = dynamicKafkaProducerManager.getProducerTransferConfigMap();

        //遍历existContainers，与configDynamicKafkaList比较，如果存在，则忽略，如果不存在，则删除
        for (String key : baseTopicToNewTopicMap.keySet()) {

            boolean exist = false;

            for (ConfigDynamicKafka configDynamicKafka : configDynamicKafkaList) {
                String baseTopic = configDynamicKafka.getOriginalTopic() + "_"
                        + configDynamicKafka.getTenantId();
                if (key.equalsIgnoreCase(baseTopic)) {
                    exist = true;
                }
            }

            if (!exist) {
                log.info("Dynamic Kafka 分发配置缓存删除：{}", key);
                baseTopicToNewTopicMap.remove(key);
            }
        }

        //如果先启动了消费者组，后取消了数据库配置
        //这个场景不要将消费者组进行下线，防止topic中的消息没有消费完

    }

}

