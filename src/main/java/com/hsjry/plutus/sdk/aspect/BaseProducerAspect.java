package com.hsjry.plutus.sdk.aspect;

import com.hsjry.plutus.sdk.component.EnvComponent;
import com.hsjry.plutus.sdk.mq.MqEntity;
import com.hsjry.plutus.sdk.mq.Producer;
import com.hsjry.plutus.sdk.utils.AopTargetUtils;
import lombok.Data;
import org.apache.rocketmq.common.message.Message;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * 拦截消息发送BaseProducer类的方法调用。
 * @author qyf
 * @Date 2019/7/17
 */
@Aspect
@Component
public class BaseProducerAspect {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private EnvComponent envComponent;

    @Autowired
    private Producer producer;

    /**
     * 切入点为BaseProducer的所有public方法。
     */
    @Pointcut("within(com.hsjry.plutus.sdk.jms.BaseProducer)")
    public void productor() {
    }

    /**
     * 拦截器原来的消息发送方法，改成调用本地的消息。
     *
     * @param joinpoint
     * @return
     * @throws Throwable
     */
    @Around("productor()")
    public Object beforeMethod(ProceedingJoinPoint joinpoint) throws Throwable {
        if(envComponent.isProductEnv()){
            logger.info("经检测当前环境为生产环境：{}，所有消息服务走线上服务。",envComponent.getCurActiveProfile());
            return joinpoint.proceed();
        }

        logger.info("当前环境为开发或者测试环境：{}，消息服务走线下服务。",envComponent.getCurActiveProfile());
        logger.info(">>>>>>>>>>>>拦截BaseProducer类方法调用切入点");

        // 参数列表
        Object args[] = joinpoint.getArgs();
        if (args == null || args.length == 0) {
            return null;
        }

        // 拦截消息生产者消息发送
        Object baseProducer = joinpoint.getTarget();

        // 取得该方法的参数
        Params params = this.getParams(joinpoint);

        // 消息主题
        String topic = (String) AopTargetUtils.getFieldValue(baseProducer, "topic");
        Properties properties = (Properties) AopTargetUtils.getMethodValue(baseProducer,"getProperties");
        // 生产者id
        String pid = AopTargetUtils.getPid(properties, topic);

        MqEntity mqEntity = this.producer.getMqProductorInfo(pid);
        try {
            Message rocketMsg = new Message();
            rocketMsg.setBody(params.getContent().getBytes("UTF-8"));
            rocketMsg.setKeys(params.getKeyId());
            // 为了区分不同的环境（TEST1,TEST2,DEV)而不产生干扰
            // 若为开发环境则默认为只能由本机消费
            rocketMsg.setTopic(envComponent.getCurActiveProfile() + "_" + topic);
            rocketMsg.setTags(params.getTag());
            rocketMsg.setDelayTimeLevel(params.getDelayTime());
            logger.info("拦截消息分组：{}，生产者主题：{}",envComponent.getGroup(pid), rocketMsg.getTopic());
            // 方法名
            String methodName = joinpoint.getSignature().getName();
            if("sendMsgOneway".equals(methodName)){
                mqEntity.getDefaultMQProducer().sendOneway(rocketMsg);
            }else{
                mqEntity.getDefaultMQProducer().send(rocketMsg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return true;
    }


    /**
     * 获取方法的参数值。
     *
     * @param joinpoint
     * @return
     */
    private Params getParams(JoinPoint joinpoint){
        Params params = new Params();
        Object args[] = joinpoint.getArgs();

        // 方法名
        String methodName = joinpoint.getSignature().getName();

        // 拦截消息生产者消息发送
        Object baseProducer = joinpoint.getTarget();
        if("sendMsg".equals(methodName)||"sendMsgAsy".equals(methodName)||"sendMsgOneway".equals(methodName)){
            // 方法：public void sendMsg(String content, String tag, int delayTime)
            if(args.length == 3){
                params.setContent((String)args[0]);
//                params.setKeyId((String)args[1]);
                params.setTag((String)args[1]);
                params.setDelayTime((int)args[2]);
                return params;
            }

            // 方法：public void sendMsg(String content, String tag, String keyId, int delayTime)
            if(args.length == 4){
                params.setContent((String)args[0]);
                params.setTag((String)args[1]);
                params.setKeyId((String)args[2]);
                params.setDelayTime((int)args[3]);
                return params;
            }

            // 不存在的方法调用忽略
            logger.error("不存在的方法调用:{},{} ",methodName, args);
            return params;
        }

        // 方法：public void sendSecondMsg(String content, String keyId, int second)
        if("sendSecondMsg".equals(methodName)){
            if(args.length == 3) {
                params.setKeyId((String) args[1]);
                params.setContent((String) args[0]);
                params.setDelayTime((int) args[2]);
                return params;
            }

            // 方法：public void sendSecondMsg(String content, String type, String keyId, int delayTime)
            if(args.length == 4){
                params.setContent((String)args[0]);
                params.setTag((String)args[1]);
                params.setKeyId((String)args[2]);
                params.setDelayTime((int)args[3]);
            }
        }

        // 方法：public void sendMsgMillionSecond(String content, String keyId, long delayTime)
        // 方法：public void sendParseMsg(String content,String keyId,int second)
        // 方法：public void sendGrabMsg(String content,String keyId,int second)
        if("sendMsgMillionSecond".equals(methodName)
                || "sendGrabMsg".equals(methodName)
                || "sendParseMsg".equals(methodName)){
            params.setContent((String)args[0]);
            params.setKeyId((String)args[1]);
            if(args[2] instanceof Long){
                params.setDelayTime(((Long)args[2]).intValue());
            }else{
                params.setDelayTime((int)args[2]);
            }

            return params;
        }


        logger.error("不存在的方法调用:{},{} ",methodName, args);
        return params;
    }


    @Data
    class Params{
        private String content;
        private String tag;
        private String keyId;
        private int delayTime;
    }

    public static void main(String[] args){
        Long a = 123355L;
        System.out.println(a.intValue());
    }
}

