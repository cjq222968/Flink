package com.stream.common;

import com.stream.common.utils.ConfigUtils;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.common
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:08
 * @Description: TODO
 * @Version: 1.0
 */
public class CommonTest {
    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }
}
