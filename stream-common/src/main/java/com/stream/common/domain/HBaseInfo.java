package com.stream.common.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.common.domain
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  18:46
 * @Description: TODO
 * @Version: 1.0
 */
public class HBaseInfo implements Serializable {
    private String address;
    private String port;
    private List<HBaseTable> tableList = new ArrayList<>();

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HBaseTable implements Serializable {
        private String tableName;
        private List<RowKey> rowKeyList;
        private String tableFamily = "info";

        public HBaseTable(String tableName, RowKey... rowKeyList) {
            this.tableName = tableName;
            this.rowKeyList = Arrays.asList(rowKeyList);
        }

        public HBaseTable(String tableName, List<RowKey> rowKeyList) {
            this.tableName = tableName;
            this.rowKeyList = rowKeyList;
        }
    }

    @Data
    @NoArgsConstructor
    public static class RowKey implements Serializable {
        private List<RowKeyItem> rowKeyItemList = new ArrayList<>();
        private String rowKeyItemFieldsSeparator = "";
        private Boolean rowKeyNeedMd5 = false;

        public RowKey(String fieldName) {
            rowKeyItemList.add(new RowKeyItem(fieldName, false));
        }

        public RowKey(String fieldName, Boolean rowKeyItemNeedMd5) {
            rowKeyItemList.add(new RowKeyItem(fieldName, rowKeyItemNeedMd5));
        }

        public RowKey(RowKeyItem... list) {
            this.rowKeyItemList.addAll(Arrays.asList(list));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RowKeyItem implements Serializable {
        private String fieldName;
        private Boolean rowKeyItemNeedMd5 = false;
    }
}
