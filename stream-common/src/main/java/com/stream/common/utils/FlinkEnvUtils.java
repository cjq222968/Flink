package com.stream.common.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.common.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  17:28
 * @Description: TODO
 * @Version: 1.0
 */
public class FlinkEnvUtils {
    public static StreamExecutionEnvironment getFlinkRuntimeEnv(){
        if (CommonUtils.isIdeaEnv()){
            System.err.println("Action Local Env");
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
