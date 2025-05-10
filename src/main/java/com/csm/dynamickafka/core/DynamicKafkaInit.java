package com.csm.dynamickafka.core;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Resource;

import com.csm.dynamickafka.entity.ConfigDynamicKafka;
import com.csm.dynamickafka.enums.DeleteFlagEnum;
import com.csm.dynamickafka.service.IConfigDynamicKafkaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @author caishumin
 * @version 1.0
 * @description
 * @createTime 2025年03月22日 15:02:00
 */
@Component
@Slf4j
public class DynamicKafkaInit {

    private static final CountDownLatch INIT_LATCH = new CountDownLatch(1);

    public static boolean isInitialized() {
        return INIT_LATCH.getCount() == 0;
    }

    public static void awaitInitialization() throws InterruptedException {
        INIT_LATCH.await();
    }

    /**
     * 注入Spring上下文用于获取Bean
     */
    private final ApplicationContext applicationContext;

    private final DynamicKafkaConsumerManager dynamicKafkaConsumerManager;

    private final DynamicKafkaProducerManager dynamicKafkaProducerManager;

    private final IConfigDynamicKafkaService iConfigDynamicKafkaService;

    public DynamicKafkaInit(ApplicationContext applicationContext, DynamicKafkaConsumerManager dynamicKafkaConsumerManager, DynamicKafkaProducerManager dynamicKafkaProducerManager, IConfigDynamicKafkaService iConfigDynamicKafkaService) {
        this.applicationContext = applicationContext;
        this.dynamicKafkaConsumerManager = dynamicKafkaConsumerManager;
        this.dynamicKafkaProducerManager = dynamicKafkaProducerManager;
        this.iConfigDynamicKafkaService = iConfigDynamicKafkaService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        //获取所有 Bean 的名称
        String[] beanNames = applicationContext.getBeanDefinitionNames();

        for (String beanName : beanNames) {
            //如果有@KafkaListener注解，则将其添加到监听器管理器中，这里需要自行编辑
            if (beanName.toLowerCase().endsWith("listener")) {
                Object bean = applicationContext.getBean(beanName);
                //处理代理类
                Class<?> targetClass = AopUtils.getTargetClass(bean);
                for (Method method : targetClass.getDeclaredMethods()) {
                    KafkaListener annotation = method.getAnnotation(KafkaListener.class);
                    if (annotation != null) {
                        for (String topic : annotation.topics()) {
                            dynamicKafkaConsumerManager.addListenerMethod(topic,
                                    new KafkaListenerMethodWrapper(bean, method, annotation));
                        }
                    }
                }
            }
        }

        log.info("已解析的@KafkaListener方法：" + dynamicKafkaConsumerManager.getListenerMethods()
                .keySet());

        //查询出所有非逻辑删除的动态kafka配置信息
        LambdaQueryWrapper<ConfigDynamicKafka> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(ConfigDynamicKafka::getDeleteFlag, DeleteFlagEnum.ENABLE.getCode());
        List<ConfigDynamicKafka> configDynamicKafkaList = iConfigDynamicKafkaService.list(
                queryWrapper);
        if (CollectionUtils.isEmpty(configDynamicKafkaList)) {
            INIT_LATCH.countDown();
            return;
        }

        //添加转发配置到内存中
        for (ConfigDynamicKafka configDynamicKafka : configDynamicKafkaList) {
            dynamicKafkaProducerManager.addTopicMap(configDynamicKafka.getOriginalTopic(),
                    configDynamicKafka.getNewTopic(), configDynamicKafka.getTenantId());
        }

        //遍历配置，添加到动态kafka监听器中
        for (ConfigDynamicKafka configDynamicKafka : configDynamicKafkaList) {
            dynamicKafkaConsumerManager.addConsumer(configDynamicKafka.getNewTopic(),
                    configDynamicKafka.getOriginalTopic(), configDynamicKafka.getNewGroupId(),
                    configDynamicKafka.getTenantId());
        }

        //初始化结束，只有执行了这个，task才会以每分钟一次正式执行
        INIT_LATCH.countDown();
    }

}

