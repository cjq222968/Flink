package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.retailersv1.func
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15 19:14
 * @Description: 解决连接管理与日志问题的 HBase 维度表更新功能类
 * @Version: 1.0
 */
public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject, JSONObject> {
    // 日志工具（生产级规范，避免System.err）
    private static final Logger log = LoggerFactory.getLogger(MapUpdateHbaseDimTableFunc.class);

    // 每个Flink并行实例独立持有资源（避免线程安全问题）
    private HbaseUtils hbaseUtils;
    private Connection hbaseConn;
    private final String hbaseNameSpace;
    private final String zkHostList;
    // 连接状态标记（线程私有，无共享风险）
    private boolean isConnAvailable = false;


    /**
     * 构造函数：传入独立配置，确保每个实例初始化参数隔离
     * @param cdhZookeeperServer ZooKeeper集群地址
     * @param cdhHbaseNameSpace HBase命名空间
     */
    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
        log.info("初始化函数实例：ZooKeeper={}，命名空间={}", zkHostList, hbaseNameSpace);
    }


    /**
     * Flink算子初始化：每个实例独立创建HBase连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 调用独立连接初始化方法，避免代码冗余
        initHbaseConnection();
    }


    /**
     * 独立初始化HBase连接（确保线程安全，支持重连复用）
     */
    private void initHbaseConnection() {
        try {
            // 销毁旧资源（防止重连时泄漏）
            if (hbaseUtils != null) {
                hbaseUtils.close();
                log.warn("初始化前关闭旧HbaseUtils实例（实例={}）", this.hashCode());
            }
            if (hbaseConn != null && !hbaseConn.isClosed()) {
                hbaseConn.close();
                log.warn("初始化前关闭旧HBase连接（实例={}）", this.hashCode());
            }

            // 新建独立连接
            hbaseUtils = new HbaseUtils(zkHostList);
            hbaseConn = hbaseUtils.getConnection();

            // 验证连接有效性
            if (hbaseConn != null && !hbaseConn.isClosed()) {
                isConnAvailable = true;
                log.info("HBase连接初始化成功（实例={}），命名空间={}", this.hashCode(), hbaseNameSpace);
            } else {
                isConnAvailable = false;
                log.error("HBase连接初始化失败（实例={}）：连接为空或已关闭", this.hashCode());
            }
        } catch (IOException e) {
            isConnAvailable = false;
            log.error("HBase连接IO异常（实例={}，ZooKeeper={}）", this.hashCode(), zkHostList, e);
        } catch (Exception e) {
            isConnAvailable = false;
            log.error("HBase连接初始化未知异常（实例={}）", this.hashCode(), e);
        }
    }


    /**
     * 核心数据处理逻辑：分操作类型处理，全链路日志
     */
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // 1. 连接不可用时直接跳过，避免无效操作
        if (!isConnAvailable) {
            log.error("HBase连接不可用（实例={}），跳过数据：{}", this.hashCode(), jsonObject);
            return jsonObject;
        }

        // 2. 校验操作类型字段（核心字段非空校验）
        String op = jsonObject.getString("op");
        if (op == null) {
            log.error("数据缺少'op'字段（实例={}），数据：{}", this.hashCode(), jsonObject);
            return jsonObject;
        }
        log.debug("开始处理操作（实例={}，op={}），数据预览：{}",
                this.hashCode(), op, jsonObject.getString("source"));

        // 3. 分操作类型处理，捕获异常避免任务崩溃
        try {
            switch (op) {
                case "d":
                    handleDelete(op, jsonObject);
                    break;
                case "r":
                case "c":
                    handleCreateOrRead(op, jsonObject);
                    break;
                default:
                    handleOtherOp(op, jsonObject);
                    break;
            }
        } catch (Exception e) {
            log.error("操作处理失败（实例={}，op={}），数据：{}",
                    this.hashCode(), op, jsonObject, e);
            // 异常时标记连接无效，触发下次重连
            isConnAvailable = false;
        }

        return jsonObject;
    }


    /**
     * 处理删除操作：含表存在性校验、删除结果二次校验
     * @param op 操作类型（固定为"d"）
     * @param json 输入数据JSON
     */
    private void handleDelete(String op, JSONObject json) {
        // 连接有效性二次校验，无效则终止
        if (!checkAndReconnect()) {
            return;
        }

        // 字段校验：确保before和sink_table非空
        JSONObject before = json.getJSONObject("before");
        if (before == null) {
            log.error("删除操作缺少'before'字段（op={}），数据：{}", op, json);
            return;
        }
        String sinkTable = before.getString("sink_table");
        if (sinkTable == null || sinkTable.isEmpty()) {
            log.error("删除操作'before'缺少'sink_table'（op={}），数据：{}", op, json);
            return;
        }

        // 构建完整表名，执行删除逻辑
        String fullTable = getFullTableName(sinkTable);
        log.debug("删除操作准备（op={}）：表={}", op, fullTable);
        try {
            if (hbaseUtils.tableIsExists(fullTable)) {
                log.info("删除前检查表存在（op={}）：表={}，开始删除", op, fullTable);
                hbaseUtils.deleteTable(fullTable);

                // 二次校验删除结果，确保操作生效
                if (!hbaseUtils.tableIsExists(fullTable)) {
                    log.info("删除操作成功（op={}）：表={}已不存在", op, fullTable);
                } else {
                    log.error("删除操作异常（op={}）：表={}仍存在", op, fullTable);
                }
            } else {
                log.warn("删除操作跳过（op={}）：表={}不存在", op, fullTable);
            }
        } catch (Exception e) {
            log.error("删除操作执行异常（op={}，表={}）", op, fullTable, e);
        }
    }


    /**
     * 处理新增（c）/读（r）操作：含表创建并发校验
     * @param op 操作类型（"c"或"r"）
     * @param json 输入数据JSON
     */
    private void handleCreateOrRead(String op, JSONObject json) {
        if (!checkAndReconnect()) {
            return;
        }

        // 字段校验：确保after和sink_table非空
        JSONObject after = json.getJSONObject("after");
        if (after == null) {
            log.error("{}操作缺少'after'字段，数据：{}", op, json);
            return;
        }
        String sinkTable = after.getString("sink_table");
        if (sinkTable == null || sinkTable.isEmpty()) {
            log.error("{}操作'after'缺少'sink_table'，数据：{}", op, json);
            return;
        }

        // 构建完整表名，执行创建/跳过逻辑
        String fullTable = getFullTableName(sinkTable);
        log.debug("{}操作准备：表={}", op, fullTable);
        try {
            if (hbaseUtils.tableIsExists(fullTable)) {
                log.info("{}操作跳过创建：表={}已存在", op, fullTable);
            } else {
                log.info("{}操作开始创建表：表={}，命名空间={}", op, sinkTable, hbaseNameSpace);
                hbaseUtils.createTable(hbaseNameSpace, sinkTable);

                // 校验创建结果，确保表已存在
                if (hbaseUtils.tableIsExists(fullTable)) {
                    log.info("{}操作创建表成功：表={}", op, fullTable);
                } else {
                    log.error("{}操作创建表失败：表={}仍不存在", op, fullTable);
                }
            }
        } catch (Exception e) {
            log.error("{}操作执行异常（表={}）", op, fullTable, e);
        }
    }


    /**
     * 处理其他操作：先删旧表再建新表，全链路日志追踪
     * @param op 操作类型（非"d"/"r"/"c"）
     * @param json 输入数据JSON
     */
    private void handleOtherOp(String op, JSONObject json) {
        if (!checkAndReconnect()) {
            return;
        }

        // 1. 处理旧表删除
        JSONObject before = json.getJSONObject("before");
        String oldTable = before != null ? before.getString("sink_table") : null;
        if (oldTable != null && !oldTable.isEmpty()) {
            String fullOldTable = getFullTableName(oldTable);
            try {
                if (hbaseUtils.tableIsExists(fullOldTable)) {
                    log.info("其他操作（op={}）开始删除旧表：{}", op, fullOldTable);
                    hbaseUtils.deleteTable(fullOldTable);
                    log.info("其他操作（op={}）删除旧表完成：{}", op, fullOldTable);
                } else {
                    log.warn("其他操作（op={}）跳过删除：旧表{}不存在", op, fullOldTable);
                }
            } catch (Exception e) {
                log.error("其他操作（op={}）删除旧表异常：{}", op, fullOldTable, e);
            }
        } else {
            log.error("其他操作（op={}）缺少旧表信息：{}", op, json);
        }

        // 2. 处理新表创建
        JSONObject after = json.getJSONObject("after");
        if (after == null) {
            log.error("其他操作（op={}）缺少'after'字段：{}", op, json);
            return;
        }
        String newTable = after.getString("sink_table");
        if (newTable == null || newTable.isEmpty()) {
            log.error("其他操作（op={}）缺少新表名：{}", op, json);
            return;
        }

        String fullNewTable = getFullTableName(newTable);
        try {
            if (hbaseUtils.tableIsExists(fullNewTable)) {
                log.info("其他操作（op={}）跳过创建：新表{}已存在", op, fullNewTable);
            } else {
                log.info("其他操作（op={}）开始创建新表：{}", op, fullNewTable);
                hbaseUtils.createTable(hbaseNameSpace, newTable);
                log.info("其他操作（op={}）创建新表完成：{}", op, fullNewTable);
            }
        } catch (Exception e) {
            log.error("其他操作（op={}）创建新表异常：{}", op, fullNewTable, e);
        }
    }


    /**
     * 连接有效性校验+自动重连：线程私有，避免共享资源冲突
     * @return 连接有效返回true，无效返回false
     */
    private boolean checkAndReconnect() {
        try {
            // 连接有效直接返回
            if (isConnAvailable && hbaseConn != null && !hbaseConn.isClosed()) {
                return true;
            }

            // 连接无效，触发重连
            log.warn("HBase连接无效（实例={}），开始自动重连", this.hashCode());
            initHbaseConnection(); // 复用初始化方法，避免代码冗余
            if (isConnAvailable) {
                log.info("HBase重连成功（实例={}）", this.hashCode());
                return true;
            } else {
                log.error("HBase重连失败（实例={}）", this.hashCode());
                return false;
            }
        } catch (Exception e) {
            log.error("连接校验/重连异常（实例={}）", this.hashCode(), e);
            isConnAvailable = false;
            return false;
        }
    }


    /**
     * 统一构建完整表名：避免拼接错误，确保日志与操作表名一致
     * @param tableName 基础表名（无命名空间）
     * @return 完整表名（命名空间:表名）
     */
    private String getFullTableName(String tableName) {
        return hbaseNameSpace + ":" + tableName;
    }


    /**
     * Flink算子销毁：双重释放资源，避免泄漏
     */
    @Override
    public void close() throws Exception {
        super.close();
        // 1. 优先关闭HbaseUtils（内部已释放Connection+Admin等资源）
        try {
            if (hbaseUtils != null) {
                hbaseUtils.close();
                log.info("关闭HbaseUtils实例（实例={}）", this.hashCode());
            }
        } catch (Exception e) {
            log.error("关闭HbaseUtils异常（实例={}）", this.hashCode(), e);
        }

        // 2. 二次关闭Connection（双重保险，防止HbaseUtils释放不彻底）
        try {
            if (hbaseConn != null && !hbaseConn.isClosed()) {
                hbaseConn.close();
                log.info("关闭HBase Connection（实例={}）", this.hashCode());
            }
        } catch (IOException e) {
            log.error("关闭HBase Connection IO异常（实例={}）", this.hashCode(), e);
        }

        // 3. 标记连接无效，避免残留状态
        isConnAvailable = false;
        log.info("函数实例销毁完成（实例={}）", this.hashCode());
    }
}