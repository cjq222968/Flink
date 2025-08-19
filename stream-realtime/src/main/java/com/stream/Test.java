package com.stream;

import com.stream.common.utils.ConfigUtils;
import lombok.SneakyThrows;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  15:38
 * @Description: TODO
 * @Version: 1.0
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
        System.err.println(kafka_botstrap_servers);
    }
}
