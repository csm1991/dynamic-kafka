package com.csm.dynamickafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.csm.dynamickafka.core.DynamicKafkaProducerManager;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Kafka生产者
 *
 * @author Simon Cai
 * @version 1.0
 * @since 2025-05-10
 */
@Component
public class MyKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final DynamicKafkaProducerManager dynamicKafkaProducerManager;

    public MyKafkaProducer(KafkaTemplate<String, String> kafkaTemplate, DynamicKafkaProducerManager dynamicKafkaProducerManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.dynamicKafkaProducerManager = dynamicKafkaProducerManager;
    }

    public void sendMessage(String topic, String message) {

        //消息一定是json格式，所以直接转换
        JSONObject jsonObject = JSON.parseObject(message);

        //获取对应的newTopic
        String newTopic = dynamicKafkaProducerManager.getNewTopic(topic, jsonObject);

        //发送消息，如果需要监听发送结果，自行实现
        kafkaTemplate.send(new ProducerRecord<>(newTopic, UUID.randomUUID().toString(), message));
    }
}
