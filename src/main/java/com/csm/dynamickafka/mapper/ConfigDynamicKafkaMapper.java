package com.csm.dynamickafka.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.csm.dynamickafka.entity.ConfigDynamicKafka;
import org.apache.ibatis.annotations.Mapper;

/**
 * ConfigDynamicKafka仓储层接口
 *
 * @author Simon Cai
 * @version 1.0
 * @since 2025-05-10
 */
@Mapper
public interface ConfigDynamicKafkaMapper extends BaseMapper<ConfigDynamicKafka> {
}