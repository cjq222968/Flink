package com.stream.common.utils;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.common.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  16:35
 * @Description: 配置文件工具类，用于安全地读取配置参数
 * @Version: 1.0
 */
public final class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);
    private static Properties properties;

    static {
        try {
            properties = new Properties();
            // 加载配置文件（使用try-with-resources确保流关闭）
            try (InputStream is = ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.properties")) {
                if (is == null) {
                    throw new IOException("配置文件 common-config.properties 未找到");
                }
                properties.load(is);
            }
            logger.info("配置文件 common-config.properties 加载成功");
        } catch (IOException e) {
            logger.error("加载配置文件失败，程序退出", e);
            System.exit(1);
        }
    }

    /**
     * 读取字符串配置，自动处理空值
     * @param key 配置键
     * @return 配置值（trim后），若不存在则返回null
     */
    public static String getString(String key) {
        String value = properties.getProperty(key);
        return Strings.isNullOrEmpty(value) ? null : value.trim();
    }

    /**
     * 读取整数配置
     * @param key 配置键
     * @return 配置值对应的整数
     * @throws NumberFormatException 若配置值不是整数
     * @throws IllegalArgumentException 若配置键不存在
     */
    public static int getInt(String key) {
        String value = getString(key);
        if (value == null) {
            throw new IllegalArgumentException("配置键 " + key + " 不存在");
        }
        return Integer.parseInt(value);
    }

    /**
     * 读取整数配置，支持默认值
     * @param key 配置键
     * @param defaultValue 当配置不存在或为空时的默认值
     * @return 配置值对应的整数，或默认值
     * @throws NumberFormatException 若配置值不是整数
     */
    public static int getInt(String key, int defaultValue) {
        String value = getString(key);
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value);
    }

    /**
     * 读取长整数配置
     * @param key 配置键
     * @return 配置值对应的长整数
     * @throws NumberFormatException 若配置值不是长整数
     * @throws IllegalArgumentException 若配置键不存在
     */
    public static long getLong(String key) {
        String value = getString(key);
        if (value == null) {
            throw new IllegalArgumentException("配置键 " + key + " 不存在");
        }
        return Long.parseLong(value);
    }

    /**
     * 读取长整数配置，支持默认值
     * @param key 配置键
     * @param defaultValue 当配置不存在或为空时的默认值
     * @return 配置值对应的长整数，或默认值
     * @throws NumberFormatException 若配置值不是长整数
     */
    public static long getLong(String key, long defaultValue) {
        String value = getString(key);
        return Strings.isNullOrEmpty(value) ? defaultValue : Long.parseLong(value);
    }






}
