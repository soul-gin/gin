package com.gin.table;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * 从DataStream(流计算)或者DataSet转换成Table(动态)
 *
 * @author gin
 * @date 2021/3/10
 */
public class UdfTableFlatMapTest {

    public static void main(String[] args) {
        //创建流式计算的上下文环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建Table API的上下文环境
        //方式1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        //连接socket获取输入的数据
        DataStreamSource<String> text = streamEnv.socketTextStream("node01", 8888, "\n");
        //输入数据源, 输入数据每行的行名为: line
        Table table = tableEnv.fromDataStream(text, "line");
        // 注册UDF, 名为: mySplit
        tableEnv.registerFunction("mySplit", new UdfTableFlatMap());
        // 使用自定义的 mySplit 函数, 处理数据源名为的 line 字段, 并定义处理后的两个字段名,
        Table result = table.flatMap("mySplit(line)").as("word, word_count")
                .groupBy("word").select("word, word_count.sum").as("单词, 单词次数");
        //将 table 对象中的数据输出
        // toRetractStream (都可以使用, 推荐(流,批计算均可))
        // toAppendStream (状态中没有 insert 和 update 才可以使用, 如:批计算, 无状态)
        DataStream<Tuple2<Boolean, Row>> resDataStream = tableEnv.toRetractStream(result, Row.class);
        resDataStream
                // Tuple2<Boolean, Row> 中的boolean表示是否是新的计算结果,
                // false表示是计算过程中被修改的状态数据, 旧数据的值
                // true表示是计算过程中, 修改之后的结果
                .filter((FilterFunction<Tuple2<Boolean, Row>>) value -> value.f0)
                .returns(TypeInformation.of(new TypeHint<Tuple2<Boolean, Row>>() {}))
                .print();

        try {
            //启动其中一个即可
            //streamEnv.execute("table stream")
            tableEnv.execute("udf");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
