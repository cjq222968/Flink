package com.label.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.label.domain.DimBaseCategory;
import com.label.domain.DimCategoryCompare;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.label.func
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:26
 * @Description: TODO
 * @Version: 1.0
 */
public class MapDeviceAndSearchMarkModelFunc extends RichMapFunction<JSONObject, JSONObject> {
    private final double deviceRate;
    private final double searchRate;

    private transient Map<String, DimBaseCategory> categoryMap;
    private transient List<DimCategoryCompare> dimCategoryCompares;
    private transient Connection connection;

    public MapDeviceAndSearchMarkModelFunc(double deviceRate, double searchRate) {
        this.deviceRate = deviceRate;
        this.searchRate = searchRate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));

        // 加载 base_category
        String sql1 = "select b3name, b1name from realtime_v1.base_category3;";
        List<DimBaseCategory> categories = JdbcUtils.queryList2(connection, sql1, DimBaseCategory.class, true);
        categoryMap = new HashMap<>();
        for (DimBaseCategory c : categories) {
            categoryMap.put(c.getB3name(), c);
        }

        // 加载 compare_dic
        String sql2 = "select category_name, search_category from realtime_v1.category_compare_dic;";
        dimCategoryCompares = JdbcUtils.queryList2(connection, sql2, DimCategoryCompare.class, true);
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        // 你的业务逻辑不变
        String os = jsonObject.getString("os");
        String[] labels = os.split(",");
        String judge_os = labels[0];
        jsonObject.put("judge_os", judge_os);

        if ("iOS".equals(judge_os)) {
            jsonObject.put("device_18_24", round(0.7 * deviceRate));
            // ... 其他逻辑
        } else if ("Android".equals(judge_os)) {
            // ...
        }

        String searchItem = jsonObject.getString("search_item");
        if (searchItem != null && !searchItem.isEmpty()) {
            DimBaseCategory category = categoryMap.get(searchItem);
            if (category != null) {
                jsonObject.put("b1_category", category.getB1name());
            }
        }

        String b1Category = jsonObject.getString("b1_category");
        if (b1Category != null && !b1Category.isEmpty()) {
            for (DimCategoryCompare cmp : dimCategoryCompares) {
                if (b1Category.equals(cmp.getCategoryName())) {
                    jsonObject.put("searchCategory", cmp.getSearchCategory());
                    break;
                }
            }
        }

        // searchCategory 逻辑不变
        String searchCategory = jsonObject.getString("searchCategory");
        if (searchCategory == null) searchCategory = "unknown";

        switch (searchCategory) {
            case "时尚与潮流":
                jsonObject.put("search_18_24", round(0.9 * searchRate));
                // ...
                break;
            // 其他 case
            default:
                jsonObject.put("search_18_24", 0);
                // ...
        }

        return jsonObject;
    }

    private double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) connection.close();
    }
}