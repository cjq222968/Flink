package com.stream;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  15:37
 * @Description: TODO
 * @Version: 1.0
 */
public class StreamTest {
    @SneakyThrows
    public static void main(String[] args) {

        System.err.println(MD5Hash.getMD5AsHex("15".getBytes(StandardCharsets.UTF_8)));



    }
}
