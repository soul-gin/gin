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
 * 使用 keyBy 分流的窗口
 * @author gin
 * @date 2021/3/2
 */
public class TumblingTimeWindowKeyed {

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
                //通过keyBy分流好的无界流
                //注意: 尽量不要使用数字作为 keyBy 参数, 不利于后续使用key时的类型推断
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 滚动窗口: 每5秒内的数据统计一次
                // 滚动窗口是一种特殊的滑动窗口, 第一个参数: 统计时间片, 第二个参数: 滑动的时间长度
                //.timeWindow(Time.seconds(5))
                .timeWindow(Time.seconds(5), Time.seconds(5))
                //如果希望一个window批次中的数据
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> old, Tuple2<String, Integer> current) throws Exception {
                        old.setField(old.f1 + current.f1, 1);
                        return old;
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        //获取窗口开始时间
                        long startTime = context.window().getStart();
                        //获取窗口结束时间
                        long endTime = context.window().getEnd();
                        System.out.println("startTime=" + startTime + " endTime" + endTime);
                        Connection conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "gin", "123456");
                        PreparedStatement updatePst = conn.prepareStatement("update wc set word_count = ? where word = ?");
                        PreparedStatement insertPst = conn.prepareStatement("insert into wc values(?,?)");

                        //迭代器里面只有一个元素
                        //注意: 这里不能使用 while( input.iterator().hasNext() ) 迭代, 指针位置没有更新, 一直为true
                        //会死循环
                        Tuple2<String, Integer> value = input.iterator().next();
                        //更新数据
                        updatePst.setInt(1, value.f1);
                        updatePst.setString(2, value.f0);
                        updatePst.execute();
                        //如果没有更新,表示记录不存在
                        if (updatePst.getUpdateCount() == 0) {
                            //插入数据
                            insertPst.setString(1, value.f0);
                            insertPst.setInt(2, value.f1);
                            insertPst.execute();
                        }

                        updatePst.close();
                        insertPst.close();
                        conn.close();
                        out.collect(value);
                    }
                }).print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }


}
