package com.gin.table;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;


/**
 * 从一个文件中导入表结构(structure)(常用于批计算)(静态)
 *
 * @author gin
 * @date 2021/3/10
 */
public class CreateTableFromFile {

    public static void main(String[] args) {
        //创建流式计算的上下文环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建Table API的上下文环境
        //方式1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        String[] columns = new String[]{"id", "name", "score"};
        TypeInformation[] types = new TypeInformation[]{TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(Double.class)};
        CsvTableSource csvTableSource = new CsvTableSource("./data/tableExamples", columns, types);

        tableEnv.registerTableSource("exampleTab", csvTableSource);
        Table table = tableEnv.scan("exampleTab");
        table.printSchema();
        //方式1
        Table query = tableEnv.sqlQuery("SELECT name,score FROM exampleTab WHERE score > 70");
        //方式2
        Table query2 = table.select("name,score").where("score > 70");
        //方式3
        Table query3 = table.select("*").filter("score > 70");
        DataStream<Tuple2<Boolean, Row>> resDataStream = tableEnv.toRetractStream(query3, Row.class);
        resDataStream
                .filter((FilterFunction<Tuple2<Boolean, Row>>) value -> value.f0)
                .returns(TypeInformation.of(new TypeHint<Tuple2<Boolean, Row>>() {}))
                .print();

        try {
            tableEnv.execute("file table");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
