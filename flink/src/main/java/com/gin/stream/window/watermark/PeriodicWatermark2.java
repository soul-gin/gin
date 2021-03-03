package com.gin.stream.window.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * 使用flink实现的水印
 * 周期性水印（Periodic Watermark）根据事件或者处理时间周期性的触发水印生成器(Assigner)，
 * 默认100ms，每隔100毫秒自动向流里注入一个Watermark
 *
 * @author gin
 * @date 2021/3/3
 */
public class PeriodicWatermark2 {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //每隔100ms(默认值)向数据流中插入一个 Watermark
        env.getConfig().setAutoWatermarkInterval(100);
        //2、在往socket发射数据的时候 必须携带时间戳
        long delay = 3000L;
        //
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* 连接socket获取输入的数据
        // nc -lk 8888
        // netstat -natp | grep 8888
        // 测试数据格式, 时间戳 + 数据
10000 hello gin
11000 hello soul
12000 hello soul
10000 hello gin
15000 hello soul
        */
        SingleOutputStreamOperator<String> stream = env.socketTextStream("node01", 8888)
                .assignTimestampsAndWatermarks(
                        //指定水印延迟3秒: Time.seconds(3)
                        //使用flink实现好的: BoundedOutOfOrdernessTimestampExtractor
                        new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(3)) {
                            //将数据中的时间字段提取出来，转成Long类型,不改变输入的数据样式
                            @Override
                            public long extractTimestamp(String line) {
                                //第一位为时间戳, 通过空格切分(使用数据源的EventTime 替换 flink的默认时间)
                                String[] split = line.split(" ");
                                return Long.parseLong(split[0]);
                            }
                        });

        stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(" ");
                for (String word : splits) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 因为是基于keyed stream之上的窗口
                // 所以必须要对应的key累积到 3 个, 才会发生计算
                .timeWindow(Time.seconds(3), Time.seconds(3))
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
                        //注意有 flink 有自己定义的时间区间算法(毫秒)
                        // TimeWindow.getWindowStartWithOffset
                        // timestamp - (timestamp - offset + windowSize) % windowSize
                        // 10000 - ( 10000 - 0 + 3000) % 3000
                        // 10000 - 1000 = 9000 起始窗口位置(9秒)
                        System.out.println("startTime=" + startTime + " endTime" + endTime);
                        out.collect(input.iterator().next());
                    }
                })
                .print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }

}
