package com.hsjry.plutus.sdk.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetAddress;

/**
 * 环境变量组件。
 *
 * @author qyf
 * @Date 2019/7/17
 */
@Component
public class EnvComponent {


//    @Value("${mq.server:172.16.54.183:9876}")
    @Value("${mq.server:127.0.0.1:9876}")
    private String mqServer;

    /**
     * 默认为开发环境
     */
    private String curActiveProfile = "dev";
    /**
     * 正式环境
     */
    private String curActiveProfileProd = "prod";

    /**
     * 是否开启只能本机消费
     */
    @Value("${local.consume.only:1}")
    private String localConsumeOnly;

    @Autowired
    private Environment env;

    @PostConstruct
    void init() {
        String[] profiles = env.getActiveProfiles();
        if (profiles.length > 0) {
            curActiveProfile = profiles[0];
        }else{
            curActiveProfile="default";
        }
    }

    /**
     * 检测是否为生产环境。
     */
    public boolean isProductEnv() {

        return curActiveProfileProd.equals(curActiveProfile);
    }

    /**
     * 消息是否只能本机消费。
     *
     * @return
     */
    public boolean isLocalConsumeOnly() {
        return localConsumeOnly.equals("1");
    }

    /**
     * 检测是否为开发环境。
     */
    public boolean isDevEnv() {
        return !curActiveProfileProd.equals(curActiveProfile);
    }

    /**
     * 取得本机名。
     *
     * @return
     */
    public String getHostName() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostName();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }


    /**
     * 取得当前运行环境的配置变量。
     *
     * @return
     */
    public String getCurActiveProfile() {
        return curActiveProfile;
    }

    /**
     * 取得消息服务器地址。
     *
     * @return
     */
    public String getMqServer() {
        return this.mqServer;
    }

    /**
     * 取得分组topic，若为开发环境则默认为只能由本机消费
     *
     * @return
     */
    public String getGroup(String pid) {
        // 消息分组
        String group = this.getCurActiveProfile() + "_" + pid;

        // 若为开发环境则默认为只能由本机消费
        if (this.isDevEnv() && this.isLocalConsumeOnly()) {
            group = this.getCurActiveProfile() + "_" + pid + "_" + this.getHostName();
        }

        return group;
    }
}
