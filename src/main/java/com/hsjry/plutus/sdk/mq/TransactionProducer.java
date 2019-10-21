package com.hsjry.plutus.sdk.mq;

import com.aliyun.openservices.ons.api.bean.TransactionProducerBean;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.aliyun.openservices.ons.api.transaction.TransactionStatus;
import com.hsjry.plutus.sdk.component.EnvComponent;
import com.hsjry.plutus.sdk.utils.AopTargetUtils;
import com.hsjry.plutus.sdk.utils.MqHookerUtils;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * 初始化事务消息生产者列表。
 * @author qyf
 * @Date 2019/7/31
 */
@Component
public class TransactionProducer {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 取得所有替换前（阿里云消息sdk）事务生产者beans列表
     */
    @Autowired(required = false)
    private List<? extends TransactionProducerBean> transactionProducerBeanList;

    /**
     * 取得所有替换前（阿里云消息sdk）事务生产者beans列表
     */
    private Map<String, MqEntity> mqEntityMap = new HashMap<>();

    @Autowired
    private EnvComponent envComponent;

    /**
     * 替换阿里云sdk的消息队列beans为线下的消息对象（开发和测试环境，线上环境不变）。
     */
    @PostConstruct
    public void init() {
        if (envComponent.isProductEnv()) {
            logger.info("经检测当前环境为生产环境：{}，所有事务消息服务走线上服务。", envComponent.getCurActiveProfile());
            return;
        }

        if(CollectionUtils.isEmpty(transactionProducerBeanList)){
            logger.info("没有检测到事务消息生产者列表.......");
            return;
        }

        logger.info("开发和测试环境的事务消息生产者创建开始.........................");
        transactionProducerBeanList.forEach(producerBean -> this.createProductor(producerBean));

        // 注册钩子，以便退出时关闭消息队列
        this.registHooker(this.mqEntityMap);
        logger.info("开发和测试环境的事务消息生产者创建结束。");
    }

    /**
     * 注册钩子，以便退出时关闭消息队列。
     *
     * @param mqEntityMap
     */
    private void registHooker(Map<String, MqEntity> mqEntityMap) {
        List<DefaultMQProducer> list = new ArrayList<>();
        mqEntityMap.values().forEach(mqEntity -> list.add(mqEntity.getTransactionMQProducer()));
        Runtime.getRuntime().addShutdownHook(new Thread(new MqHookerUtils(list)));
    }

    /**
     * 创建事务生产者。
     *
     * @param transactionProducerBean
     */
    private void createProductor(TransactionProducerBean transactionProducerBean) {
        MqEntity mqEntity = new MqEntity();

        // 通过代理类取得目标类实例（BeihuiProducer）
        Object beiHuiProducer = AopTargetUtils.getTarget(transactionProducerBean);

        // 消息主题
        String topic = (String) AopTargetUtils.getFieldValue(beiHuiProducer, "topic");
        mqEntity.setTopic(topic);

        Properties properties = (Properties) AopTargetUtils.getMethodValue(beiHuiProducer, "getProperties");
        // 生产者id
        String pid = AopTargetUtils.getPid(properties, topic);
        mqEntity.setPid(pid);
        // 消费者id
        String cid = properties.getProperty("ConsumerId");
        mqEntity.setCid(cid);
        String group = envComponent.getGroup(pid);

        TransactionMQProducer transactionProducer = new TransactionMQProducer(group);
        transactionProducer.setNamesrvAddr(envComponent.getMqServer());
        TransactionCheckListener transactionCheckListener=null;
        transactionProducer.setTransactionCheckListener(new TransactionCheckListener() {
            @Override
            public LocalTransactionState checkLocalTransactionState(MessageExt messageExt) {

                LocalTransactionChecker localTransactionChecker=transactionProducerBean.getLocalTransactionChecker();

                com.aliyun.openservices.ons.api.Message messageOns=new com.aliyun.openservices.ons.api.Message();
                messageOns.setBody(messageExt.getBody());
                TransactionStatus transactionStatus=localTransactionChecker.check(messageOns);
                LocalTransactionState localTransactionState;
                switch (transactionStatus) {
                    case CommitTransaction:
                        localTransactionState=LocalTransactionState.COMMIT_MESSAGE;
                        break;
                    case RollbackTransaction :
                        localTransactionState=LocalTransactionState.ROLLBACK_MESSAGE;
                        break;
                    default :
                        localTransactionState=LocalTransactionState.UNKNOW;
                }
                return localTransactionState;
            }
        });

        // 保持目标实例和新的消息生产者
        mqEntity.setTransactionMQProducer(transactionProducer);
        mqEntity.setSubTransactionProducerBean(transactionProducerBean);

        try {
            transactionProducer.start();
        } catch (Exception e) {
            logger.info("该事务消息的分组已经创建过，无需再建：{}----->{}", group, e.getMessage());
        }

        mqEntityMap.put(pid, mqEntity);
        logger.info("创建事务主题：{},分组:{}的生产者。", topic, group);
    }

    /**
     * 取得线下rockmq所有的消息发送者实体。
     *
     * @return
     */
    public MqEntity getMqProductorInfo(String pid) {
        return this.mqEntityMap.get(pid);
    }


    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("Producer");
        producer.setNamesrvAddr("172.16.54.183:9876");
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        producer.setSendMsgTimeout(1000000000);
        producer.start();
        Message msg = new Message("plutus_topic_dev",
                "TagA",String.valueOf(System.currentTimeMillis()),
                ("Hello RocketMQ " + 1).getBytes(RemotingHelper.DEFAULT_CHARSET)
        );
        SendResult sendResult = producer.send(msg);
        System.out.printf("%s%n", sendResult);
        producer.shutdown();
    }
}
