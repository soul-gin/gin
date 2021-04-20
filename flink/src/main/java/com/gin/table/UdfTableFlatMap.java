package com.gin.table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
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
        //已废弃
        //return Types.ROW(Types.STRING(), Types.INT())
        //在用
        return getRowTypeInfo();
    }

    /**
     * 生成Row类型的TypeInformation.
     */
    private static RowTypeInfo getRowTypeInfo() {
        // 2个字段
        TypeInformation[] types = new TypeInformation[2];
        String[] fieldNames = new String[2];
        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.INT_TYPE_INFO;

        fieldNames[0] = "word";
        fieldNames[1] = "count";

        return new RowTypeInfo(types, fieldNames);
    }

    /**
     * 自定义处理数据方式
     * @param line 源数据
     */
    public void eval(String line){
        //自定义逻辑:
        //单词作为第一个字段, 单词出现的次数作为第二个字段
        String[] split = line.split(" ");
        for (String s : split) {
            //根据业务设计row, 设计2个字段
            Row row = new Row(2);
            // 第一个字段为单词
            row.setField(0, s);
            // 第二个字段映射为 1
            row.setField(1, 1);
            // 收集数据
            collect(row);
        }

    }
}
