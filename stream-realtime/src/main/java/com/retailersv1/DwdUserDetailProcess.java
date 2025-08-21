package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.DimJoinFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 用户明细DWD层处理：
 * 1. 从ODS层读取原始用户CDC数据
 * 2. 数据清洗（过滤无效数据、处理空值）
 * 3. 格式标准化（统一字段命名、时间格式）
 * 4. 关联维度数据（用户等级维度）
 * 5. 写入DWD层Kafka主题
 */
public class DwdUserDetailProcess {
    // 配置参数（仅保留用户相关配置）
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String ODS_USER_CDC_TOPIC = ConfigUtils.getString("kafka.ods.user.cdc.topic");
    private static final String DWD_USER_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.user.detail.topic");
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String USER_LEVEL_DIM_TABLE = ConfigUtils.getString("hbase.dim.user.level.table");

    public static void main(String[] args) throws Exception {
        // 打印关键配置（便于调试）
        System.out.println("DWD_USER_DETAIL_TOPIC: " + DWD_USER_DETAIL_TOPIC);
        System.out.println("CDH_KAFKA_SERVER: " + CDH_KAFKA_SERVER);

        // 1. 初始化Flink执行环境
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env); // 配置Checkpoint、并行度等
        env.disableOperatorChaining(); // 禁用算子链，便于调试和监控

        // 2. 从ODS层Kafka读取原始用户CDC数据
        KafkaSource<String> userKafkaSource = KafkaUtils.buildKafkaSource(
                CDH_KAFKA_SERVER,
                ODS_USER_CDC_TOPIC,
                "user-cdc-group",
                OffsetsInitializer.earliest()
        );
        DataStreamSource<String> odsUserSource = env.fromSource(
                userKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "ods_user_cdc_source"
        );

        // 打印原始ODS数据（确认源头数据）
        odsUserSource.print("原始用户ODS数据: ");

        // 3. 数据清洗与转换
        SingleOutputStreamOperator<JSONObject> cleanedUserStream = odsUserSource
                // 转换为JSON对象
                .map(JSONObject::parseObject)
                .name("parse_user_json")
                .uid("parse_user_json")
                // 过滤无效数据（非删除操作+用户ID非空）
                .filter(json -> {
                    String userId = json.getString("id");
                    String op = json.getString("op");
                    boolean isValid = userId != null && !userId.isEmpty() && !"d".equals(op);
                    if (!isValid) {
                        System.out.println("被过滤的无效用户数据: " + json.toJSONString());
                    }
                    return isValid;
                })
                .name("filter_invalid_user_data")
                .uid("filter_invalid_user_data")
                // 格式标准化处理
                .map(json -> {
                    JSONObject result = new JSONObject();
                    // 统一字段命名
                    result.put("user_id", json.getString("id"));
                    result.put("user_name", json.getString("username"));
                    result.put("phone", json.getString("phone"));
                    result.put("id_card", json.getString("id_card"));
                    result.put("email", json.getString("email"));
                    result.put("level_id", json.getString("level_id"));

                    // 时间格式标准化（时间戳→yyyy-MM-dd HH:mm:ss）
                    long updateTime = json.getLong("update_time");
                    result.put("update_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(updateTime)));

                    // 保留操作类型
                    result.put("op_type", json.getString("op"));
                    return result;
                })
                .name("normalize_user_format")
                .uid("normalize_user_format");

        // 打印清洗后的标准化数据
        cleanedUserStream.print("清洗后的用户标准化数据: ");

        // 4. 关联用户等级维度数据
        MapStateDescriptor<String, JSONObject> userLevelDimDescriptor = new MapStateDescriptor<>(
                "user_level_dim_broadcast",
                String.class,
                JSONObject.class
        );
        BroadcastStream<JSONObject> userLevelDimBroadcast = getDimBroadcastStream(
                env,
                USER_LEVEL_DIM_TABLE,
                userLevelDimDescriptor
        );

        SingleOutputStreamOperator<JSONObject> userWithLevelDimStream = cleanedUserStream
                .connect(userLevelDimBroadcast)
                .process(new DimJoinFunction(userLevelDimDescriptor, "level_id")) // 按level_id关联
                .name("join_user_level_dim")
                .uid("join_user_level_dim");

        // 打印关联维度后的最终数据
        userWithLevelDimStream.print("最终用户DWD层数据: ");

        // 5. 输出到DWD层Kafka
        userWithLevelDimStream
                .map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, DWD_USER_DETAIL_TOPIC))
                .name("dwd_user_detail_sink")
                .uid("dwd_user_detail_sink");

        // 6. 启动任务
        env.execute("DWD User Detail Process");
    }

    /**
     * 获取维度表广播流（从HBase读取维度数据）
     */
    private static BroadcastStream<JSONObject> getDimBroadcastStream(
            StreamExecutionEnvironment env,
            String tableName,
            MapStateDescriptor<String, JSONObject> descriptor) {

        DataStreamSource<JSONObject> dimSource = env.addSource(
                HbaseUtils.buildHBaseDimSource(CDH_ZOOKEEPER_SERVER, CDH_HBASE_NAME_SPACE, tableName),
                "hbase_" + tableName + "_source"
        );

        // 打印从HBase读取的维度数据
        dimSource.print("从HBase读取的" + tableName + "维度数据: ");

        return dimSource.broadcast(descriptor);
    }
}