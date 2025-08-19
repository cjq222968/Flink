package com.stream.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.domain
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:52
 * @Description: TODO
 * @Version: 1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class MySQLMessageInfo {
    private String id;
    private String op;
    private String db_name;
    private String log_before;
    private String log_after;
    private String t_name;
    private String ts;
}
