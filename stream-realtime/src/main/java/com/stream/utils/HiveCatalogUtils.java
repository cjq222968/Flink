package com.stream.utils;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:56
 * @Description: TODO
 * @Version: 1.0
 */
public class HiveCatalogUtils {
    private static final String HIVE_CONF_DIR = ConfigUtils.getString("hive.conf.dir");

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", HIVE_CONF_DIR);
    }
}
