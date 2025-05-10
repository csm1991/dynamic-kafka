## 动态Kafka配置模块

## 设计背景
当平台多租户共享Kafka集群时，单租户流量激增会导致：
1. 消费者线程被大流量租户独占
2. 消息堆积影响其他租户正常消费
3. 无法动态扩容指定租户的消费能力

## 业务价值
- ✅ 租户间消费资源隔离
- ✅ 动态调整消费能力无需重启
- ✅ 生产消费链路自动化分流
- ✅ 消费者启停配置化管理

## 功能介绍
- 该模块提供了动态Kafka配置功能，通过配置表动态控制Kafka消费者启动，实现多租户的生产者、消费者分流。
- 该模块想要解决的问题是当某个租户的单量上升，触发Kafka消费瓶颈的时候，会导致单个租户影响了平台其他租户的正常消费，从而影响了其他租户的正常使用
- 该模块不主动销毁消费者(但是提供了对应的能力，用户请选择性使用)，因为程序无法主动判断该消费者是否已经消费完所有消息，以及是否不再会有新消息被投递到配置的动态队列

### 包结构
```
dynamic-kafka
├── core/                                   # 核心控制逻辑
│   ├── DynamicKafkaConsumerManager.java    # 消费者生命周期管理
│   ├── DynamicKafkaProducerManager.java    # 生产者生命周期管理
│   ├── DynamicKafkaInit.java               # 初始化入口
│   ├── DynamicKafkaTask.java               # 定时任务调度
│   └── KafkaListenerMethodWrapper.java     # 监听器封装
├── entity/                                 # 数据实体
│   └── ConfigDynamicKafka.java             # 动态配置实体
├── mapper/                                 # 数据访问层
│   └── ConfigDynamicKafkaMapper.java    
└── service/                                # 服务层
    ├── IConfigDynamicKafkaService.java     # 服务接口
    └── impl/                               # 服务实现
        └── ConfigDynamicKafkaServiceImpl.java
```

### 核心功能
1. **动态配置管理**
    - 通过ConfigDynamicKafka实体维护消费者组、主题映射关系
    - 支持运行时通过数据库配置创建消费者

2. **消费者生命周期管理**
    - DynamicKafkaConsumerManager实现消费者启停控制
    - 自动同步数据库配置变更（1分钟轮询）

3. **监听器绑定机制**
    - 通过扫描@KafkaListener获取消费者业务逻辑，无需手动复制业务逻辑
    - KafkaListenerMethodWrapper保存新队列与消费者关联关系

## 快速开始

### 基础配置
1. 创建配置表
```sql
CREATE TABLE `config_dynamic_kafka` (
   `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
   `tenant_id` bigint unsigned DEFAULT NULL COMMENT '租户对应的uid',
   `original_topic` varchar(500) DEFAULT NULL COMMENT '原始topic',
   `new_topic` varchar(500) DEFAULT NULL COMMENT '新的topic',
   `new_group_id` varchar(500) DEFAULT NULL COMMENT '新的消费者组',
   `delete_flag` int DEFAULT '0' COMMENT '删除标志位 0：正常 1 删除',
   PRIMARY KEY (`id`)
) COMMENT '动态Kafka配置表';
```

2. 添加Kafka配置
```yaml
spring:
  kafka:
    bootstrap-servers: your_kafka_servers
```

### 使用流程
```java
// 1、确认想要实现动态管理的消费者有正确使用@KafkaListener注解，如下所示
@KafkaListener(id = KafkaConstants.TOPIC, topics = KafkaConstants.TOPIC, groupId = KafkaConstants.GROUP)
public void yourListener(List<ConsumerRecord<Object, Object>> recordList, Acknowledgment ack) {
}

// 2、数据库配置示例（config_dynamic_kafka表）
INSERT INTO config_dynamic_kafka 
(tenant_id, original_topic, new_topic, new_group_id) 
VALUES 
(租户ID, '原始队列(用来匹配消费者业务逻辑，以及生产者转发)', '新的队列，建议格式：original_topic+租户ID', '新消费者组，如果不填会自动根据UUID生成一个，建议填写原本的消费者组');

// 3、动态启动消费者前置动作
DynamicKafkaInit类会监听【应用启动完成动作】，应用启动成功后会自动扫描@KafkaListener的Bean方法，并自动缓存到内存中

// 4、动态启动消费者
// 4.1、应用启动时自动启动消费者
DynamicKafkaInit类会在应用启动完成后，通过读取数据库配置来自动启动消费者
// 4.2、定时扫描启动新消费者
DynamicKafkaTask类会在DynamicKafkaInit初始化结束后，每一分钟轮训一次数据库，并与内存中的消费者进行匹配，增量的自动启动消费者

// 5、生产者分流
MyKafkaProducer会根据数据库配置，结合租户ID进行消息转发，所以生产者一定要使用这两个工具类来进行消息发送

// 6、销毁消费者
DynamicKafkaConsumerManager.removeConsumer方法可以销毁消费者，但是需要手动调用
```
