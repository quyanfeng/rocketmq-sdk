package com.hsjry.plutus.sdk.mq;

import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.aliyun.openservices.ons.api.bean.TransactionProducerBean;
import lombok.Data;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;

/**
 * 封装消息实体。
 * @author qyf
 * @Date 2019/7/17
 */
@Data
public class MqEntity {
    private String topic;
    private String pid;
    private String cid;

    //普通消息
    /** Producer bean对象 */
    private ProducerBean subProducerBean;

    /** 替换为原生的rocketMq的消息生产者 */
    private DefaultMQProducer DefaultMQProducer;

    //事务消息
    /** TransactionProducer bean对象 */
    private TransactionProducerBean subTransactionProducerBean;

    /** 替换为原生的rocketMq的事务消息生产者 */
    private TransactionMQProducer transactionMQProducer;
}
