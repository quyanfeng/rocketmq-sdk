package com.hsjry.plutus.sdk.utils;

import java.util.UUID;

/**
 * @Description:UUID工具类
 * @Author：shenjm25081
 * @Date：9:23 2018/8/22
 */
public class UUIDUtil {
    public static String getUUID() {
        String uuid = UUID.randomUUID().toString().trim().replaceAll("-", "").toUpperCase();
        return uuid;
    }
}
