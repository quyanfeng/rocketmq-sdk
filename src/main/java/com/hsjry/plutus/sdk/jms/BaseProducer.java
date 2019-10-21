package com.hsjry.plutus.sdk.jms;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.OnExceptionContext;
import com.aliyun.openservices.ons.api.SendCallback;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.hsjry.plutus.sdk.utils.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * producer基础类
 * @Author qyf 
 * @Date 2019/7/17 17:41
 **/
public class BaseProducer extends ProducerBean {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String topic;

    public BaseProducer(String risktopic) {
        this.topic = risktopic;
    }

    public void sendMsg(String content, String tag, String keyId, int delayTime) {
        Message msg = new Message(topic, tag, content.getBytes());
        if (keyId != null) {
            msg.setKey(keyId);
        }

        try {
            //定时
            if (delayTime != 0) {
                msg.setStartDeliverTime(System.currentTimeMillis() + (60 * 1000 * delayTime));
            }
            SendResult sendResult = this.send(msg);
            assert sendResult != null;
            logger.debug("消息发送成功:keyId：{}, tag:{}, msgId:{} ", keyId, tag, sendResult.getMessageId());
        } catch (ONSClientException e) {
            logger.error("消息发送失败，e:{}", e);
        }
    }

    public void sendMsg(String content, String tag, int delayTime) {
       this.sendMsg(content,tag, UUIDUtil.getUUID(), delayTime);
    }

    /**
     * 异步发送消息
     * 可靠异步发送：发送方发出数据后，不等接收方发回响应，接着发送下个数据包的通讯方式；
     * 特点：速度快；有结果反馈；数据可靠；
     * 应用场景：异步发送一般用于链路耗时较长,对 rt响应时间较为敏感的业务场景,例如用户视频上传后通知启动转码服务,转码完成后通知推送转码结果等；
     * @param content
     * @return
     */
    public boolean sendMsgAsy(String content, String tag, int delayTime) {
        Long startTime = System.currentTimeMillis();
        Message message = new Message(topic, tag, content.getBytes());
        //定时
        if (delayTime != 0) {
            message.setStartDeliverTime(System.currentTimeMillis() + (60 * 1000 * delayTime));
        }
        this.sendAsync(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                ///消息发送成功
                System.out.println("send message success. topic=" + sendResult.getMessageId());
            }

            @Override
            public void onException(OnExceptionContext context) {
                //消息发送失败
                System.out.println("send message failed. execption=" + context.getException());
            }
        });
        Long endTime = System.currentTimeMillis();
        System.out.println("单次生产耗时："+(endTime-startTime)/1000);
        return true;
    }

    /**
     * 单向发送
     * 单向发送：只负责发送消息，不等待服务器回应且没有回调函数触发，即只发送请求不等待应答；此方式发送消息的过程耗时非常短，一般在微秒级别；
     * 特点：速度最快，耗时非常短，毫秒级别；无结果反馈；数据不可靠，可能会丢失；
     * 应用场景：适用于某些耗时非常短，但对可靠性要求并不高的场景，例如日志收集；
     * @return
     */
    public boolean sendMsgOneway(String content, String tag, int delayTime) {

        Long startTime = System.currentTimeMillis();
        Message message = new Message(topic, tag, content.getBytes());
        //定时
        if (delayTime != 0) {
            message.setStartDeliverTime(System.currentTimeMillis() + (60 * 1000 * delayTime));
        }
        this.sendOneway(message);
        Long endTime = System.currentTimeMillis();
        System.out.println("单次生产耗时："+(endTime-startTime)/1000);
        return true;
    }
}
