package com.stream.common.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.crypto.SecureUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.stream.common.domain.HBaseInfo;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.common.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  18:45
 * @Description: TODO
 * @Version: 1.0
 */
public class HbaseUtils {
    private Connection connection;
    private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class.getName());

    public HbaseUtils(String zookeeper_quorum) throws Exception {
        org.apache.hadoop.conf.Configuration entries = HBaseConfiguration.create();
        entries.set(HConstants.ZOOKEEPER_QUORUM, zookeeper_quorum);
        // setting hbase "hbase.rpc.timeout" and "hbase.client.scanner.timeout" Avoidance scan timeout
        entries.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "1800000");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "1800000");
        // setting hbase "hbase.hregion.memstore.flush.size" buffer flush
        entries.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, "128M");
        entries.set("hbase.incremental.wal", "true");
        entries.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "3600000");
//        entries.set(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,"1200000");
        this.connection = ConnectionFactory.createConnection(entries);
    }

    public Connection getConnection() {
        return connection;
    }

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
                System.err.println("构建表列族：" + s);
                tableDescriptorBuilder.setColumnFamily(build);
            }
        } else {
            ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build();
            System.err.println("构建表列族：info");
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor build = tableDescriptorBuilder
                .build();
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
        System.err.println("表 ：" + tableName + (b? " 存在" : " 不存在"));
        return b;
    }

    public void getHbaseNameSpaceAllTablesList(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();
        TableName[] tableNamesByNamespace = admin.listTableNamesByNamespace(nameSpace);
        ArrayList<TableName> tableNames = new ArrayList<>(Arrays.asList(tableNamesByNamespace));
        if (!tableNames.isEmpty()) {
            for (TableName tableName : tableNames) {
                System.err.println("table -> " + tableName);
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
        System.err.println("禁用表：" + tableName);
        admin.deleteTable(TableName.valueOf(tableName));
        System.err.println("删除表 ：" + tableName);
        return tableIsExists(tableName);
    }

    public String getString(String tableName, String rowkey) throws IOException {
        Get get = new Get(rowkey.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        return result.toString();
    }

    public boolean isConnect() {
        return!connection.isClosed();
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
//        List list1 = IteratorUtils.toList(iterator);
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
        System.err.println("耗时 " + (l1 - l));
        return list;
    }


    public void deleteByRowkeys(String tableName, ArrayList<Delete> deletes) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.delete(deletes);
    }

/*
    public String delSnapshots(String snapshotName) throws IOException {
        for (SnapshotDescription listSnapshot : connection.getAdmin().listSnapshots()) {
            if (!listSnapshot.getName().isEmpty() && listSnapshot.getName().equals(snapshotName)){
                connection.getAdmin().deleteSnapshot(snapshotName);
                return "delete of -> "+ snapshotName;
            }
        }
        return "The "+snapshotName+" does not Exist !";
    }
*/

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
        return "表 -> " + tableName + "共计: " + rowCount + " 条" + " , 统计耗时 -> " + (stopTime - startTime);
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
                System.err.println("del -> " + table.getName());
            }
        }
    }

    /**
     * 关闭HBase连接资源
     * 释放Connection连接，避免资源泄漏
     */
    public void close() {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                    LOG.info("HBase connection has been closed successfully");
                }
            } catch (IOException e) {
                LOG.error("Failed to close HBase connection", e);
            }
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        HbaseUtils hbaseUtils = null;
        try {
            hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
            // 示例操作
            // hbaseUtils.dropHbaseNameSpace("GMALL_FLINK_2207");
            // System.err.println(hbaseUtils.tableIsExists("realtime_v2:dim_user_info"));
            hbaseUtils.deleteTable("ns_zxn:dim_base_category1");
            // hbaseUtils.getHbaseNameSpaceAllTablesList("realtime_v2");
        } catch (Exception e) {
            LOG.error("HBase operation failed", e);
        } finally {
            if (hbaseUtils != null) {
                hbaseUtils.close();
            }
        }
    }

    // 补充 buildHBaseDimSource 方法，返回 SourceFunction<JSONObject> 用于 Flink 作为数据源
    public static SourceFunction<JSONObject> buildHBaseDimSource(String zookeeperQuorum, String nameSpace, String tableName) {
        return new SourceFunction<JSONObject>() {
            private Connection connection = null;
            private final AtomicBoolean isRunning = new AtomicBoolean(true);

            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {
                // 初始化 HBase 连接
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
                connection = ConnectionFactory.createConnection(config);

                Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
                Scan scan = new Scan();
                ResultScanner scanner = table.getScanner(scan);
                Iterator<Result> iterator = scanner.iterator();

                while (isRunning.get() && iterator.hasNext()) {
                    Result result = iterator.next();
                    JSONObject json = new JSONObject();
                    // 将 Result 中的数据转换为 JSONObject，这里简单示例假设列族为 "info"，可根据实际列族调整
                    result.listCells().forEach(cell -> {
                        String qualifier = Bytes.toString(cloneQualifier(cell));
                        String value = Bytes.toString(cloneValue(cell));
                        json.put(qualifier, value);
                    });
                    ctx.collect(json);
                }

                // 关闭资源
                if (scanner != null) {
                    scanner.close();
                }
                if (table != null) {
                    table.close();
                }
            }

            @Override
            public void cancel() {
                isRunning.set(false);
                // 关闭 HBase 连接
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        LOG.error("关闭 HBase 连接失败", e);
                    }
                }
            }
        };
    }
}