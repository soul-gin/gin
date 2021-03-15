package com.gin.table;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.table.api.java.*;
import org.apache.flink.streaming.api.*;


/**
 * 从DataStream(流计算)或者DataSet转换成Table(动态)
 *
 * @author gin
 * @date 2021/3/10
 */
public class CreateTableFromDataStream {

    public static void main(String[] args) {
        //创建流式计算的上下文环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建Table API的上下文环境
        //方式1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        //连接socket获取输入的数据
        DataStreamSource<String> text = streamEnv.socketTextStream("node01", 8888, "\n");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordStream = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按空格 TAB 换行切分(如: splits = value.split(" ")
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        TypeInformation[] types = new TypeInformation[]{TypeInformation.of(String.class), TypeInformation.of(String.class), TypeInformation.of(Double.class)};
        //flink 中字段名通过 , (逗号)分隔(可以加空格)
        Table table = tableEnv.fromDataStream(wordStream, "word, count");
        //打印表结构
        table.printSchema();
        //查询
        Table result = table.groupBy("word").select("word, count.count");
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
            tableEnv.execute("table stream");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
