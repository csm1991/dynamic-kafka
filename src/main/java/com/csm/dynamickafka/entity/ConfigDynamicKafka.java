package com.csm.dynamickafka.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.TableLogic;
import lombok.Data;

/**
 * ConfigDynamicKafka实体
 *
 * @author Simon Cai
 * @version 1.0
 * @since 2025-05-10
 */
@Data
@TableName("config_dynamic_kafka")
public class ConfigDynamicKafka {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long tenantId;
    private String originalTopic;
    private String newTopic;
    private String newGroupId;
    private Integer deleteFlag;
}