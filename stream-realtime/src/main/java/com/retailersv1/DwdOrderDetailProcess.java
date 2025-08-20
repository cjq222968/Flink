package com.retailersv1;


import org.apache.flink.connector.kafka.source.KafkaSource;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.DimJoinFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.math.BigDecimal;

/**
 * 订单明细DWD层处理：
 * 1. 从ODS层读取原始订单CDC数据
 * 2. 数据清洗（过滤无效数据、处理空值）
 * 3. 格式标准化（统一字段命名、数据类型转换）
 * 4. 关联维度数据（用户维度、商品维度）
 * 5. 写入DWD层Kafka主题
 */
public class DwdOrderDetailProcess {
    // 配置参数
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String ODS_ORDER_CDC_TOPIC = ConfigUtils.getString("kafka.ods.order.cdc.topic");
    private static final String DWD_ORDER_DETAIL_TOPIC = ConfigUtils.getString("kafka.dwd.order.detail.topic");
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");

    public static void main(String[] args) throws Exception {
        System.out.println("DWD_ORDER_DETAIL_TOPIC: " + DWD_ORDER_DETAIL_TOPIC);
        System.out.println("CDH_KAFKA_SERVER: " + CDH_KAFKA_SERVER);

        // 1. 初始化Flink执行环境
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env); // 配置Checkpoint、并行度等
        env.disableOperatorChaining(); // 禁用算子链，便于调试和监控

        // 2. 从ODS层Kafka读取原始订单CDC数据
        DataStreamSource<String> odsOrderSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        CDH_KAFKA_SERVER,
                        ODS_ORDER_CDC_TOPIC,
                        "order-cdc-group",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "ods_order_cdc_source"
        );

        // 打印原始ODS数据（便于确认源头数据是否正常）
        odsOrderSource.print("原始ODS数据: ");

        // 3. 数据清洗与转换
        SingleOutputStreamOperator<JSONObject> cleanedOrderStream = odsOrderSource
                // 转换为JSON对象
                .map(JSONObject::parseObject)
                .name("parse_json")
                .uid("parse_json")
                // 过滤无效数据
                .filter(json -> {
                    String orderId = json.getString("id");
                    String op = json.getString("op");
                    boolean isValid = orderId != null && !orderId.isEmpty() && !"d".equals(op);
                    // 打印被过滤的数据（便于排查问题）
                    if (!isValid) {
                        System.out.println("被过滤的无效数据: " + json.toJSONString());
                    }
                    return isValid;
                })
                .name("filter_invalid_data")
                .uid("filter_invalid_data")
                // 格式标准化处理
                .map(json -> {
                    JSONObject result = new JSONObject();
                    result.put("order_id", json.getString("id"));
                    result.put("user_id", json.getString("user_id"));
                    result.put("product_id", json.getString("product_id"));

                    // 金额单位转换（元→分）
                    BigDecimal amount = json.getBigDecimal("total_amount");
                    result.put("total_amount", amount.multiply(new BigDecimal(100)).intValue());

                    // 时间格式标准化
                    long createTime = json.getLong("create_time");
                    result.put("create_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(createTime)));

                    result.put("op_type", json.getString("op"));
                    return result;
                })
                .name("normalize_data_format")
                .uid("normalize_data_format");

        // 打印清洗后的标准化数据
        cleanedOrderStream.print("清洗后的标准化数据: ");

        // 4. 关联用户维度数据
        MapStateDescriptor<String, JSONObject> userDimDescriptor = new MapStateDescriptor<>(
                "user_dim_broadcast",
                String.class,
                JSONObject.class
        );
        BroadcastStream<JSONObject> userDimBroadcast = getDimBroadcastStream(env, "dim_user", userDimDescriptor);

        SingleOutputStreamOperator<JSONObject> orderWithUserDimStream = cleanedOrderStream.connect(userDimBroadcast)
                .process(new DimJoinFunction(userDimDescriptor, "user_id"))
                .name("join_user_dim")
                .uid("join_user_dim");

        // 打印关联用户维度后的数据
        orderWithUserDimStream.print("关联用户维度后的数据: ");

        // 5. 关联商品维度数据
        MapStateDescriptor<String, JSONObject> productDimDescriptor = new MapStateDescriptor<>(
                "product_dim_broadcast",
                String.class,
                JSONObject.class
        );
        BroadcastStream<JSONObject> productDimBroadcast = getDimBroadcastStream(env, "dim_product", productDimDescriptor);

        SingleOutputStreamOperator<JSONObject> orderWithAllDimStream = orderWithUserDimStream.connect(productDimBroadcast)
                .process(new DimJoinFunction(productDimDescriptor, "product_id"))
                .name("join_product_dim")
                .uid("join_product_dim");

        // 打印关联所有维度后的最终数据（即将写入DWD层的数据）
        orderWithAllDimStream.print("最终DWD层数据: ");

        // 6. 输出到DWD层Kafka
        orderWithAllDimStream
                .map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, DWD_ORDER_DETAIL_TOPIC))
                .name("dwd_order_detail_sink")
                .uid("dwd_order_detail_sink");

        // 7. 启动任务
        env.execute("DWD Order Detail Process");
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
