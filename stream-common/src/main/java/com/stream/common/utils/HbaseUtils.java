package com.stream.common.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.hbase.*;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author
 * @time: 2021/10/14 11:39
 * @className: HBaseUtils
 * @description HBase 工具类（整合 Flink 读取 HBase 维度表逻辑）
 */
public class HbaseUtils {
    private Connection connection;
    private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class.getName());

    public HbaseUtils(String zookeeper_quorum) throws Exception {
        org.apache.hadoop.conf.Configuration entries = HBaseConfiguration.create();
        entries.set(HConstants.ZOOKEEPER_QUORUM, zookeeper_quorum);
        // 超时配置
        entries.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "1800000");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "1800000");
        entries.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, "128M");
        entries.set("hbase.incremental.wal", "true");
        this.connection = ConnectionFactory.createConnection(entries);
    }

    public Connection getConnection() {
        return connection;
    }

    // -------------------- 原有 HBase 操作方法 --------------------
    public static void put(String rowKey, JSONObject value, BufferedMutator mutator) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
        mutator.mutate(put);
    }

    public static void put(String rowKey, JSONObject value) {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
    }

    public boolean createTable(String nameSpace, String tableName, String... columnFamily) throws Exception {
        boolean b = tableIsExists(tableName);
        if (b) {
            return true;
        }
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableName));
        if (columnFamily.length > 0) {
            for (String s : columnFamily) {
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(s.getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
                LOG.info("构建表列族：{}", s);
                tableDescriptorBuilder.setColumnFamily(build);
            }
        } else {
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
            LOG.info("构建表列族：info");
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor build = tableDescriptorBuilder.build();
        admin.createTable(build);
        admin.close();
        LOG.info("Create Table {}", tableName);
        return tableIsExists(tableName);
    }

    public boolean tableIsExists(String tableName) throws Exception {
        Thread.sleep(1000);
        Admin admin = connection.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        LOG.info("表 ：{} {}", tableName, (b ? " 存在" : " 不存在"));
        return b;
    }

    public void getHbaseNameSpaceAllTablesList(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();
        TableName[] tableNamesByNamespace = admin.listTableNamesByNamespace(nameSpace);
        ArrayList<TableName> tableNames = new ArrayList<>(Arrays.asList(tableNamesByNamespace));
        if (!tableNames.isEmpty()) {
            for (TableName tableName : tableNames) {
                LOG.info("table -> {}", tableName);
            }
        }
    }

    public boolean deleteTable(String tableName) throws Exception {
        boolean b = tableIsExists(tableName);
        if (!b) {
            return false;
        }
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        LOG.info("禁用表：{}", tableName);
        admin.deleteTable(TableName.valueOf(tableName));
        LOG.info("删除表 ：{}", tableName);
        return tableIsExists(tableName);
    }

    public String getString(String tableName, String rowkey) throws IOException {
        Get get = new Get(rowkey.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        return result.toString();
    }

    public boolean isConnect() {
        return !connection.isClosed();
    }

    public ArrayList<JSONObject> getAll(String tableName, long limit) throws Exception {
        long l = System.currentTimeMillis();
        if (!this.tableIsExists(tableName)) {
            throw new NullPointerException("表不存在");
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setLimit(Math.toIntExact(limit));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        ArrayList<JSONObject> list = new ArrayList<>();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            JSONObject js = new JSONObject();
            next.listCells().forEach(cell -> {
                js.put("row_key", Bytes.toString(next.getRow()));
                js.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            });
            list.add(js);
        }
        long l1 = System.currentTimeMillis();
        LOG.info("getAll 耗时 {}", (l1 - l));
        return list;
    }

    public void deleteByRowkeys(String tableName, ArrayList<Delete> deletes) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.delete(deletes);
    }

    public String getTableRows(String tableName) throws IOException {
        long rowCount = 0;
        long startTime = System.currentTimeMillis();
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = connection.getTable(tableName1);
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            rowCount += r.size();
        }
        long stopTime = System.currentTimeMillis();
        return "表 -> " + tableName + " 共计: " + rowCount + " 条 , 统计耗时 -> " + (stopTime - startTime);
    }

    @SneakyThrows
    public void dropHbaseNameSpace(String nameSpace) {
        Admin admin = connection.getAdmin();
        TableName[] tableNamesByNamespace = admin.listTableNamesByNamespace(nameSpace);
        ArrayList<TableName> tableNames = new ArrayList<>(Arrays.asList(tableNamesByNamespace));
        if (!tableNames.isEmpty()) {
            for (TableName tableName : tableNames) {
                Table table = connection.getTable(tableName);
                admin.disableTable(table.getName());
                admin.deleteTable(tableName);
                LOG.info("del -> {}", table.getName());
            }
        }
    }

    // -------------------- 新增 Flink 读取 HBase 维度表方法 --------------------
    /**
     * 从 HBase 读取维度表，转换为 Flink DataStreamSource<JSONObject>
     *
     * @param env         Flink 执行环境
     * @param zkServer    Zookeeper 地址（如 "cdh01,cdh02,cdh03"）
     * @param nameSpace   HBase 命名空间
     * @param tableName   HBase 表名
     * @return DataStreamSource<JSONObject> 包含维度数据的流
     */
    public static DataStreamSource<JSONObject> buildHBaseDimSource(
            StreamExecutionEnvironment env,
            String zkServer,
            String nameSpace,
            String tableName) {

        // 1. 配置 HBase 连接
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zkServer);
        config.set(TableInputFormat.INPUT_TABLE, nameSpace + ":" + tableName);

        // 2. 构建 TableMapper，映射 HBase 字段到 JSONObject
        TableMapper mapper = new TableMapper() {
            @Override
            public void configure(Map<String, String> properties) {
                // 可在此配置额外参数（如列族、列映射）
            }

            @Override
            public DataStream<JSONObject> mapFromTable(Result result) {
                JSONObject json = new JSONObject();
                // 解析 RowKey
                json.put("row_key", Bytes.toString(result.getRow()));
                // 解析所有列（假设列族为 "info"）
                result.listCells().forEach(cell -> {
                    String colFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String colValue = Bytes.toString(CellUtil.cloneValue(cell));
                    // 若列族固定为 "info"，可简化为直接存列名和值
                    if ("info".equals(colFamily)) {
                        json.put(colName, colValue);
                    }
                });
                return env.fromElements(json);
            }
        };

        // 3. 创建 HBaseTableSource 并生成 DataStream
        HBaseTableSource hbaseTableSource = new HBaseTableSource(config, mapper);
        return hbaseTableSource.getDataStream(env);
    }

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        HbaseUtils hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
        // 示例：删除表（可根据需要调整操作）
        hbaseUtils.deleteTable("ns_zxn:dim_base_category1");
    }
}