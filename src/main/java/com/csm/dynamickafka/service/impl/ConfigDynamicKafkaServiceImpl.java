package com.csm.dynamickafka.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.csm.dynamickafka.entity.ConfigDynamicKafka;
import com.csm.dynamickafka.mapper.ConfigDynamicKafkaMapper;
import com.csm.dynamickafka.service.IConfigDynamicKafkaService;
import org.springframework.stereotype.Service;

@Service
public class ConfigDynamicKafkaServiceImpl extends ServiceImpl<ConfigDynamicKafkaMapper, ConfigDynamicKafka>
    implements IConfigDynamicKafkaService {
}