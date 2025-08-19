package com.stream;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  15:13
 * @Description: TODO
 * @Version: 1.0
 */
public class FlinkNcMessage {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh03", 14777);

        dataStreamSource.print();

        env.execute();
    }
}
