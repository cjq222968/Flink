package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.JdbcUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessSpiltStreamToHBaseDimFunc extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSpiltStreamToHBaseDimFunc.class);
    public static final OutputTag<JSONObject> ERROR_DATA_TAG = new OutputTag<>("hbase-process-error-data") {};

    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap = new HashMap<>();
    private org.apache.hadoop.hbase.client.Connection hbaseConnection;
    private HbaseUtils hbaseUtils;
    private final String hbaseNameSpace;


    public ProcessSpiltStreamToHBaseDimFunc(MapStateDescriptor<String, JSONObject> mapStageDesc, String hbaseNameSpace) {
        this.mapStateDescriptor = mapStageDesc;
        this.hbaseNameSpace = hbaseNameSpace;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        // 1. 从 MySQL 加载初始配置
        try (Connection mysqlConn = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"))) {
            String querySQL = "select * from realtime_v1_config.table_process_dim";
            List<TableProcessDim> configList = JdbcUtils.queryList(mysqlConn, querySQL, TableProcessDim.class, true);
            for (TableProcessDim config : configList) {
                configMap.put(config.getSourceTable(), config);
            }
            LOG.info("初始配置加载完成，共{}个表配置", configList.size());
        } catch (Exception e) {
            LOG.error("MySQL 初始配置加载失败", e);
            throw e;
        }

        // 2. 初始化 HBase 连接
        try {
            hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
            hbaseConnection = hbaseUtils.getConnection();
            LOG.info("HBase 连接初始化成功，命名空间：{}", hbaseNameSpace);
        } catch (Exception e) {
            LOG.error("HBase 连接初始化失败", e);
            throw e;
        }
    }


    @Override
    public void processElement(JSONObject dataJson, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        try {
            // 1. 校验 source 字段
            JSONObject sourceObj = dataJson.getJSONObject("source");
            if (sourceObj == null || sourceObj.getString("table") == null) {
                LOG.warn("数据缺少 source/table 字段，数据={}", dataJson);
                ctx.output(ERROR_DATA_TAG, dataJson);
                return;
            }

            // 2. 提取核心信息
            String sourceTableName = sourceObj.getString("table");
            TableProcessDim localConfig = configMap.get(sourceTableName);
            ReadOnlyBroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            JSONObject broadcastConfig = broadcastState.get(sourceTableName);

            // 3. 配置校验
            if ((broadcastConfig == null && localConfig == null) || localConfig == null || !sourceTableName.equals(localConfig.getSourceTable())) {
                LOG.warn("无有效配置：源表={}，本地配置={}，广播配置={}", sourceTableName, localConfig, broadcastConfig);
                ctx.output(ERROR_DATA_TAG, dataJson);
                return;
            }

            // 4. 过滤删除操作
            String op = dataJson.getString("op");
            if ("d".equals(op)) {
                // 兼容旧版Java的空安全处理
                JSONObject beforeObj = dataJson.getJSONObject("before");
                String dataId = beforeObj != null ? beforeObj.getString("id") : "未知";
                LOG.debug("跳过删除操作：源表={}，数据ID={}", sourceTableName, dataId);
                out.collect(dataJson);
                return;
            }

            // 5. 校验 after 字段
            JSONObject afterData = dataJson.getJSONObject("after");
            if (afterData == null) {
                LOG.warn("数据缺少 after 字段，数据={}", dataJson);
                ctx.output(ERROR_DATA_TAG, dataJson);
                return;
            }

            // 6. 提取 HBase 配置
            String sinkTable = localConfig.getSinkTable();
            if (sinkTable == null || sinkTable.trim().isEmpty()) {
                LOG.warn("SinkTable 为空，源表={}，配置={}", sourceTableName, localConfig);
                ctx.output(ERROR_DATA_TAG, dataJson);
                return;
            }
            String fullHBaseTableName = hbaseNameSpace + ":" + sinkTable;
            String sinkRowKeyField = localConfig.getSinkRowKey();
            String rowKeyValue = afterData.getString(sinkRowKeyField);

            // 7. 校验行键
            if (rowKeyValue == null || rowKeyValue.trim().isEmpty()) {
                LOG.warn("行键字段为空，源表={}，行键字段={}，数据={}", sourceTableName, sinkRowKeyField, dataJson);
                ctx.output(ERROR_DATA_TAG, dataJson);
                return;
            }

            // 8. 写入 HBase
            try (Table hbaseTable = hbaseConnection.getTable(TableName.valueOf(fullHBaseTableName))) {
                byte[] rowKeyBytes = Bytes.toBytes(MD5Hash.getMD5AsHex(rowKeyValue.getBytes(StandardCharsets.UTF_8)));
                Put put = new Put(rowKeyBytes);

                for (Map.Entry<String, Object> field : afterData.entrySet()) {
                    put.addColumn(
                            Bytes.toBytes("info"),
                            Bytes.toBytes(field.getKey()),
                            Bytes.toBytes(String.valueOf(field.getValue()))
                    );
                }

                hbaseTable.put(put);
                LOG.info("HBase 写入成功：表={}，行键={}", fullHBaseTableName, Arrays.toString(rowKeyBytes));
            } catch (Exception e) {
                LOG.error("HBase 写入失败，表={}，行键={}，数据={}", fullHBaseTableName, rowKeyValue, dataJson, e);
                ctx.output(ERROR_DATA_TAG, dataJson);
                return;
            }

            // 9. 输出原始数据
            out.collect(dataJson);

        } catch (Exception e) {
            LOG.error("数据处理全局异常，数据={}", dataJson, e);
            ctx.output(ERROR_DATA_TAG, dataJson);
        }
    }


    @Override
    public void processBroadcastElement(JSONObject configJson, Context ctx, Collector<JSONObject> out) throws Exception {
        try {
            BroadcastState<String, JSONObject> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String op = configJson.getString("op");

            // 1. 校验 after 字段
            if (!configJson.containsKey("after")) {
                LOG.warn("广播配置缺少 after 字段，配置={}", configJson);
                return;
            }
            JSONObject configAfter = configJson.getJSONObject("after");

            // 2. 校验必填字段
            List<String> requiredFields = Arrays.asList("source_table", "sink_table", "sink_row_key");
            for (String field : requiredFields) {
                if (!configAfter.containsKey(field) || configAfter.getString(field) == null) {
                    LOG.warn("广播配置缺少必填字段：{}，配置={}", field, configJson);
                    return;
                }
            }

            // 3. 安全转换为 TableProcessDim
            TableProcessDim newConfig;
            try {
                newConfig = configAfter.toJavaObject(TableProcessDim.class);
            } catch (Exception e) {
                LOG.error("配置转换失败，JSON={}，异常={}", configAfter, e);
                return;
            }

            // 4. 校验转换结果
            if (newConfig == null) {
                LOG.warn("配置转换后为空，JSON={}", configAfter);
                return;
            }

            // 5. 处理配置增/删/改
            String sourceTableName = newConfig.getSourceTable();
            switch (op) {
                case "d":
                    broadcastState.remove(sourceTableName);
                    configMap.remove(sourceTableName);
                    LOG.info("广播配置删除：源表={}", sourceTableName);
                    break;
                case "c":
                case "u":
                    broadcastState.put(sourceTableName, configJson);
                    configMap.put(sourceTableName, newConfig);
                    LOG.info("广播配置新增/修改：源表={}，目标表={}", sourceTableName, newConfig.getSinkTable());
                    break;
                default:
                    LOG.warn("不支持的广播配置操作：op={}，配置={}", op, configJson);
            }

        } catch (Exception e) {
            LOG.error("广播配置处理异常，配置={}", configJson, e);
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
        // 关闭资源：按顺序释放，避免资源泄漏
        try {
            if (hbaseConnection != null) {
                hbaseConnection.close();
                LOG.info("HBase连接已关闭");
            }
        } catch (Exception e) {
            LOG.error("HBase连接关闭异常", e);
        }

        try {
            if (hbaseUtils != null) {
                hbaseUtils.close();
                LOG.info("HBase工具类已关闭");
            }
        } catch (Exception e) {
            LOG.error("HBase工具类关闭异常", e);
        }
    }
}
