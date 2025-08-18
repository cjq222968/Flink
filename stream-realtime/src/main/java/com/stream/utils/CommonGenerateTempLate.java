package com.stream.utils;

import com.stream.common.utils.ConfigUtils;
import static com.stream.utils.SiliconFlowApi.generateBadReview;
/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  18:54
 * @Description: TODO
 * @Version: 1.0
 */
public class CommonGenerateTempLate {
    private static final String COMMENT_TEMPLATE = "生成一个电商%s,商品名称为%s,20字数以内,%s不需要思考过程 ";

    private static final String COMMENT = "差评";

    private static final String API_TOKEN = ConfigUtils.getString("silicon.api.token");

    public static String GenerateComment(String comment,String productName){
        if (comment.equals(COMMENT)){
            return generateBadReview(
                    String.format(COMMENT_TEMPLATE,COMMENT, productName, "攻击性拉满,使用脏话"),
                    API_TOKEN
            );
        }
        return generateBadReview(
                String.format(COMMENT_TEMPLATE,COMMENT, productName,""),
                API_TOKEN
        );
    }
}
