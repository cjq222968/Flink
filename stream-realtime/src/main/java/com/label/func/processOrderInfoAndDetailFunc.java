package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.label.func
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:36
 * @Description: TODO
 * @Version: 1.0
 */
public class processOrderInfoAndDetailFunc extends KeyedProcessFunction<String, JSONObject, JSONObject>{
    private ValueState<Long> latestTsState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("latestTs", Long.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
        latestTsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        Long storedTs = latestTsState.value();
        long currentTs = value.getLong("create_time");

        if (storedTs == null || currentTs > storedTs) {
            latestTsState.update(currentTs);
            out.collect(value);
        }
    }
}
