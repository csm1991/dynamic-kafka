package com.csm.dynamickafka.core;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DynamicKafkaProducerManager {
    private final Map<String, String> producerTransferConfigMap = new ConcurrentHashMap<>();

    public void addTopicMap(String baseTopic, String newTopic, Long tenantId) {
        producerTransferConfigMap.put(baseTopic + "_" + tenantId, newTopic);
    }

    public String getNewTopic(String baseTopic, Long tenantId) {
        String newTopic = producerTransferConfigMap.get(baseTopic + "_" + tenantId);
        return StringUtils.isEmpty(newTopic) ? baseTopic : newTopic;
    }

    public String getNewTopic(String baseTopic, JSONObject jsonObject) {
        String tenantId = jsonObject.getString("tenantId");
        return StringUtils.isEmpty(tenantId) ? baseTopic : getNewTopic(baseTopic, Long.valueOf(tenantId));
    }

    public Map<String, String> getProducerTransferConfigMap() {
        return producerTransferConfigMap;
    }
}