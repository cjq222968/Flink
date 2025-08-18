package com.func.hdaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.func.hdaf
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-15  15:45
 * @Description: TODO
 * @Version: 1.0
 */
public class GenericUDAFMedian extends GenericUDAFAverage{
    public GenericUDAFMedian() {
        super();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)throws SemanticException{
        return super.getEvaluator(parameters);
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException{
        return super.getEvaluator(paramInfo);
    }
}
