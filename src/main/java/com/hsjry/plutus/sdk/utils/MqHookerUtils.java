package com.hsjry.plutus.sdk.utils;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 系统退出，关闭消息队列释放资源。
 *
 * @author qyf
 * @Date 2019/7/17
 */
public class MqHookerUtils implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(MqHookerUtils.class);

    private List<DefaultMQProducer> producerBeanList;
    public MqHookerUtils(List<DefaultMQProducer> producerBean){
        this.producerBeanList = producerBean;
    }

    @Override
    public void run(){
        logger.info("系统退出，关闭消息生产者释放资源。");
        producerBeanList.forEach(producerBean -> producerBean.shutdown());
    }
}
