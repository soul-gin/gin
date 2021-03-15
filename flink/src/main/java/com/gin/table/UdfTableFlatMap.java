package com.gin.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author gin
 * @date 2021/3/10
 */
public class UdfTableFlatMap extends TableFunction<Row> {

    /**
     * 重写获取返回值类型的方法
     * @return UDF返回字段类型
     */
    @Override
    public TypeInformation<Row> getResultType() {
        //设定返回字段的类型
        //return DataTypes.ROW(DataTypes.FIELD("word", DataTypes.STRING()), DataTypes.FIELD("count", DataTypes.INT()));
        return Types.ROW(Types.STRING(), Types.INT());
    }

    /**
     * 自定义处理数据方式
     * @param line 源数据
     */
    public void eval(String line){

    }
}
