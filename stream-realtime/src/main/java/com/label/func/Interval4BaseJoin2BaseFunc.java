package com.label.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.label.func
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:24
 * @Description: TODO
 * @Version: 1.0
 */
public class Interval4BaseJoin2BaseFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject>{
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        System.err.println("jsonObject1 -> "+ jsonObject1);
        System.err.println("jsonObject2 -> "+ jsonObject2);
    }
}
