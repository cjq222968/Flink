package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.label.domain
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  16:08
 * @Description: TODO
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimBaseCategory implements Serializable {
    private String id;
    private String b3name;
    private String b2name;
    private String b1name;

}
