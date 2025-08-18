package com.label.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.label.domain
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  16:21
 * @Description: TODO
 * @Version: 1.0
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategoryName;
}
