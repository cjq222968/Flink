package com.stream.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:56
 * @Description: TODO
 * @Version: 1.0
 */
public class PaimonMinioUtils {
    public static void ExecCreateMinioCatalogAndDatabases(StreamTableEnvironment tenv, String catalogName, String databaseName){
        System.setProperty("HADOOP_USER_NAME","root");
        if (catalogName.length() == 0){
            catalogName = "minio_paimon_catalog";
        }
        tenv.executeSql("CREATE CATALOG "+ catalogName +"                             " +
                "WITH                                                                   " +
                "  (                                                                    " +
                "    'type' = 'paimon',                                                 " +
                "    'warehouse' = 's3://paimon-data/',                                 " +
                "    's3.endpoint' = 'http://192.168.142.131:9000',                         " +
                "    's3.access-key' = 'X7pljEi3steavVn5h3z3',                          " +
                "    's3.secret-key' = 'KDaSxEyfSEmKiaJDBbJ6RpBxMBp6OwnRbkA8LnKL',      " +
                "    's3.connection.ssl.enabled' = 'false',                             " +
                "    's3.path.style.access' = 'true',                                   " +
                "    's3.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem',              " +
                "    's3.aws.credentials.provider' = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider' " +
                "  );");
        tenv.executeSql("use catalog "+catalogName+";");
        tenv.executeSql("create database if not exists "+databaseName+";");
    }
}
