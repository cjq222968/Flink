package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.retailersv1.domain
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  16:52
 * @Description: TODO
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim implements Serializable{
    //来源表名
    String sourceTable;
    //目标表名
    String sinkTable;
    //输出字段
    String sinkColumns;
    //数据到hbase的列族
    String sinkFamily;

    String sinkRowKey;
    String op;
}
