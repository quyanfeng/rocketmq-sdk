package com.hsjry.plutus.sdk.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AdvisedSupport;
import org.springframework.aop.framework.AopProxy;
import org.springframework.aop.support.AopUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Enumeration;
import java.util.Properties;

/**
 * 反射工具类。
 *
 * @author qyf
 * @Date 2019/7/17
 */
public class AopTargetUtils {
    private static final Logger logger = LoggerFactory.getLogger(AopTargetUtils.class);

    /**
     * 获取 目标对象。
     *
     * @param proxy 代理对象
     * @return
     * @throws Exception
     */
    public static Object getTarget(Object proxy) {
        if(!AopUtils.isAopProxy(proxy)) {
            //不是代理对象
            return proxy;
        }

        if(AopUtils.isJdkDynamicProxy(proxy)) {
            return getJdkDynamicProxyTargetObject(proxy);
        }

        //cglib
        return getCglibProxyTargetObject(proxy);
    }

    /**
     * 从jdk动态代理对象取得目标对象。
     *
     * @param proxy
     * @return
     * @throws Exception
     */
    private static Object getCglibProxyTargetObject(Object proxy) {
        try {
            Field h = proxy.getClass().getDeclaredField("CGLIB$CALLBACK_0");
            h.setAccessible(true);
            Object dynamicAdvisedInterceptor = h.get(proxy);
            Field advised = dynamicAdvisedInterceptor.getClass().getDeclaredField("advised");
            advised.setAccessible(true);
            return ((AdvisedSupport) advised.get(dynamicAdvisedInterceptor)).getTargetSource().getTarget();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 从cglib动态代理对象取得目标对象。
     *
     * @param proxy
     * @return
     * @throws Exception
     */
    private static Object getJdkDynamicProxyTargetObject(Object proxy){
        try{
            Field h = proxy.getClass().getSuperclass().getDeclaredField("h");
            h.setAccessible(true);
            AopProxy aopProxy = (AopProxy) h.get(proxy);
            Field advised = aopProxy.getClass().getDeclaredField("advised");
            advised.setAccessible(true);
            return ((AdvisedSupport)advised.get(aopProxy)).getTargetSource().getTarget();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过反射根据私有属性和类取得该类的属性值。
     *
     * @param object
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static Object getFieldValue(Object object, String fieldName) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            if (Modifier.isPrivate(field.getModifiers())) {
                field.setAccessible(true);
                return field.get(object);
            }
            return null;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过反射根据方法名和类取得该方法的返回值。
     *
     * @param object
     * @param methodName
     * @return
     */
    public static Object getMethodValue(Object object, String methodName) {
        try {
            Method method = object.getClass().getSuperclass().getDeclaredMethod(methodName);
            return method.invoke(object, null);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 取得消费者id（从properties属性中取得，因为共享平台的属性名字和业务平台的名字不一样
     * 故，需要通过反射取得。）
     * 根据topic取得对应的pic(业务系统的properties里有所有的topic，cid，pid信息)
     * 例子1：
     *    msg_notice_topic = SDHS_MSG_NOTICE_TOPIC_test2
     *    msg_notice_pid = PID_SDHS_MSG_NOTICE_test2
     * 例子2：
     *    signature_topic = SDHS_SIGNATURE_TOPIC_test2
     *    signature_pid = PID_SDHS_SIGNATURE_PRODUCT_test2
     * 例子3：
     *    back_topic=SDHS_TOPIC_BACK_LIST_test2
     *    back_pid=PID_SDHS_BACK_LIST_test2
     *
     * @param properties
     * @param topic
     * @return
     */
    public static String getPid(Properties properties, String topic){
        Enumeration<?> propertieNames =  properties.propertyNames();
        String topic1 = topic.replaceAll("_topic", "");
        String topic2 = topic;
        int index = topic.indexOf("_topic");
        if(index >= 0) {
            topic2 = topic.substring(0, index);
        }

        while(propertieNames.hasMoreElements()){
           String field = propertieNames.nextElement().toString();
           String pid = properties.getProperty(field);
           // 参考doc说明
           if(pid.startsWith("GID-")){
                if(pid.contains(topic1) || pid.contains(topic2)){
                    return pid;
                }
            }
        }

       logger.info("没有找到消费者id：topic【{}】:{}", properties.toString(), topic);
       throw new RuntimeException("没有找到消息消费者id");
    }
}
