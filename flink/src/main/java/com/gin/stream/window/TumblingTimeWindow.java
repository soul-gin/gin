package com.gin.stream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 不使用 keyBy 分流的窗口(all)
 * 应用场景较少(一般用于汇总值/排序, 与key无关)
 * @author gin
 * @date 2021/3/2
 */
public class TumblingTimeWindow {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 连接socket获取输入的数据
        // nc -lk 8888
        // netstat -natp | grep 8888
        env.socketTextStream("node01", 8888, "\n")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.split(" ");
                        for (String word : splits) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                // 未进行 keyBy 分流
                // 滚动窗口是一种特殊的滑动窗口, 第一个参数: 统计时间片, 第二个参数: 滑动的时间长度
                .timeWindowAll(Time.seconds(5), Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> old, Tuple2<String, Integer> current) throws Exception {
                        //这里后面的 word 会被覆盖, 仅仅测试 countWindowAll 用
                        //无实际业务意义
                        old.setField(old.f1 + current.f1, 1);
                        return old;
                    }
                }).print();

        env.execute("streaming word count");

    }


}
