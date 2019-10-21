package com.hsjry.plutus.sdk.mq;

import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.bean.ConsumerBean;
import com.aliyun.openservices.ons.api.bean.Subscription;
import com.hsjry.plutus.sdk.component.EnvComponent;
import com.hsjry.plutus.sdk.utils.AopTargetUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

/**
 * 初始化消费者列表。
 * @author qyf
 * @Date 2019/7/17
 */
@Component("myConsumer")
public class Consumer {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    /**取得所有替换前（阿里云消息sdk）消费者beans列表*/
    @Autowired(required = false)
    private List<ConsumerBean> consumerBeanList;

    @Autowired
    private EnvComponent envComponent;

    /**
     * 替换阿里云sdk的消息队列beans为线下的消息对象（开发和测试环境，线上环境不变）。
     *
     */
    @PostConstruct
    public void init() {
        if (envComponent.isProductEnv()) {
            logger.info("经检测当前环境为生产环境：{}，所有消息服务走线上服务。", envComponent.getCurActiveProfile());
            return;
        }

        logger.info("经检测当前环境为开发或者测试环境：{}。",envComponent.getCurActiveProfile());
        if(CollectionUtils.isEmpty(consumerBeanList)){
            logger.info("没有检测到消费者列表.......");
            return;
        }

        logger.info("开发和测试环境的消息消费者创建开始.........................");
        consumerBeanList.forEach(consumerBean -> this.createConsumer(consumerBean));
        logger.info("开发和测试环境的消息消费者创建结束。");
    }

    /**
     * 创建消费者。
     *
     * @param consumerBean
     */
    private void createConsumer(ConsumerBean consumerBean){

        // 消息主题
        Map<Subscription, MessageListener> map = consumerBean.getSubscriptionTable();
        map.keySet().stream().forEach(subscription -> {
            try {
                // 生产者id
                String pid = AopTargetUtils.getPid(consumerBean.getProperties(), subscription.getTopic());
                String group =  envComponent.getGroup(pid);
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
                consumer.setNamesrvAddr(envComponent.getMqServer());
                String topic = envComponent.getCurActiveProfile() + "_" + subscription.getTopic();
                consumer.subscribe(topic, "*");

                // 取得消费者
                MessageListener messageListener = map.get(subscription);

                // 注册消费者
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    logger.info("收到线下消息: {},{},{}", group, topic, msgs);
                    Message message = new Message();
                    // 消息内容
                    byte[] bytes = msgs.get(0).getBody();
                    message.setBody(bytes);

                    // 消息标签
                    String tags = msgs.get(0).getTags();
                    message.setTag(tags);
                    message.setKey(msgs.get(0).getKeys());
                    message.setMsgID(msgs.get(0).getMsgId());
                    // 消息主题
                    message.setTopic(topic);
                    messageListener.consume(message, new ConsumeContext());
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });

                consumer.start();
                logger.info("创建消息消费者成功:主题：{}，分组：{}",topic, group);
            } catch(Exception e){
                throw new RuntimeException(e);
            }
        });
    }

    public static void main(String[] args) throws InterruptedException, MQClientException {

        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dev_test2");

        // Specify name server addresses.
        consumer.setNamesrvAddr("http://172.16.54.183:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("DATASHARE_UP_TOPIC_DEV1", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
