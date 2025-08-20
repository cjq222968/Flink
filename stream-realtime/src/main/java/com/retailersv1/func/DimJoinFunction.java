package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: dev-test  通用维度关联函数：将事实数据与广播的维度数据关联
 * @BelongsPackage: com.retailersv1.func
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-20  08:55
 * @Description: TODO
 * @Version: 1.0
 */
public class DimJoinFunction extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {

    private final MapStateDescriptor<String, JSONObject> dimDescriptor;
    private final String keyField; // 关联字段（如user_id、product_id）

    public DimJoinFunction(MapStateDescriptor<String, JSONObject> dimDescriptor, String keyField) {
        this.dimDescriptor = dimDescriptor;
        this.keyField = keyField;
    }

    /**
     * 处理事实数据流（订单明细）
     */
    @Override
    public void processElement(
            JSONObject factData,
            ReadOnlyContext ctx,
            Collector<JSONObject> out) throws Exception {

        // 从广播状态中获取维度数据
        ReadOnlyBroadcastState<String, JSONObject> dimState = ctx.getBroadcastState(dimDescriptor);
        String key = factData.getString(keyField);

        if (key != null && dimState.contains(key)) {
            JSONObject dimData = dimState.get(key);
            // 合并维度字段（前缀区分，避免字段冲突）
            for (String keyStr : dimData.keySet()) {
                factData.put(keyField + "_" + keyStr, dimData.getString(keyStr));
            }
        }

        out.collect(factData);
    }

    /**
     * 处理维度广播流（更新维度状态）
     */
    @Override
    public void processBroadcastElement(
            JSONObject dimData,
            Context ctx,
            Collector<JSONObject> out) throws Exception {

        // 从维度数据中提取主键（假设主键字段为id）
        String dimKey = dimData.getString("id");
        if (dimKey != null) {
            BroadcastState<String, JSONObject> dimState = ctx.getBroadcastState(dimDescriptor);
            dimState.put(dimKey, dimData); // 更新维度状态
        }
    }
}

