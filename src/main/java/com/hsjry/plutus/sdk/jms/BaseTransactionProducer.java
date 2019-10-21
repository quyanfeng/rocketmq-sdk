package com.hsjry.plutus.sdk.jms;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.bean.TransactionProducerBean;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.hsjry.plutus.sdk.utils.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务producer基础类
 * @Author qyf 
 * @Date 2019/7/31 17:41
 **/
public class BaseTransactionProducer extends TransactionProducerBean {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String topic;

    public BaseTransactionProducer(String risktopic) {
        this.topic = risktopic;
    }

    public void sendMsg(String content, String tag, String keyId,LocalTransactionExecuter executer) {

        Message msg = new Message(topic, tag, content.getBytes());
        if (keyId != null) {
            msg.setKey(keyId);
        }
        try {
            SendResult sendResult = this.send(msg,executer, null);
            logger.debug("事务消息发送成功:keyId：{}, tag:{}, msgId:{} ", keyId, tag, sendResult.getMessageId());
        }catch (Exception e) {
            // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
            logger.error("事务消息发送失败，e:{}", e);
        }
    }

    public void sendMsg(String content, String tag,LocalTransactionExecuter executer) {
       this.sendMsg(content,tag, UUIDUtil.getUUID(),executer);
    }

}
