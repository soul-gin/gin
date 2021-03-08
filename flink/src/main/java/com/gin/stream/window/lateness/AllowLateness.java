package com.gin.stream.window.lateness;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 场景: 需要既实时统计, 又需要保障数据准确性
 * 缺点: 降低吞吐量(缓存历史窗口数据)
 *
 * 基于Event-Time的窗口处理流式数据，虽然提供了Watermark机制，却只能在一定程度上解决了数据乱序的问题。
 * 但在某些情况下数据可能延时会非常严重，即使通过Watermark机制也无法等到数据全部进入窗口再进行处理。
 * Flink中默认会将这些迟到的数据做丢弃处理，但是有些时候用户希望即使数据延迟并不是很严重的情况下，
 * 也能继续窗口计算，不希望对于数据延迟比较严重的数据混入正常的计算流程中，
 * 此时就需要使用Allowed Lateness机制来对迟到的数据进行额外的处理。
 * <p>
 * 例如用户大屏数据展示系统，即使正常的窗口中没有将迟到的数据进行统计，但为了保证页面数据显示的连续性，
 * 后来接入到系统中迟到比较严重的数据所统计出来的结果不希望显示在屏幕上，而是将延时数据和结果存储到数据库中，
 * 便于后期对延时数据进行分析。对于这种情况需要借助Side Output来处理，
 * 通过使用sideOutputLateData（OutputTag）来标记迟到数据计算的结果，然后使用getSideOutput（lateOutputTag）
 * 从窗口结果中获取lateOutputTag标签对应的数据，之后转成独立的DataStream数据集进行处理，
 * 创建late-data的OutputTag，再通过该标签从窗口结果中将迟到数据筛选出来Flink默认当窗口计算完毕后，
 * 窗口元素数据及状态会被清空，但是使用AllowedLateness，可以延迟清除窗口元素数据及状态，
 * 以便于当延迟数据到来的时候，能够重新计算当前窗口
 *
 * 问题1：使用AllowedLateness 方法是不是会降低flink计算的吞吐量？
 * 是的
 *
 * 问题2：直接watermark设置为5 不是也可以代替这一通操作嘛？
 * 不能代替
 * watermark设置为5的话，允许延迟5s，每次处理过去5s的窗口数据，
 * 延迟比较高，如果使用这通操作，每次处理过去2s的数据，实时性比较高，
 * 当有新的延迟数据，即时计算，对于计算实时性比较高的场景还得使用这一通操作
 *
 * 问题3：watermark（5s）+滑动窗口（滑动间隔2s）能够实现这通计算？
 * 不行
 * 案例：每隔5s统计各个卡口最近5s的车流量（滑动窗口），计算实时性小于2（ps：当10s的数据来了，
 * 8s之前的数据必须处理完），允许数据延迟5s，数据延迟超过5s的数据放入到侧输出流中
 *
 * @author gin
 * @date 2021/3/3
 */
public class AllowLateness {

    public static void main(String[] args) {
        try {
            //获取运行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            //根据数据时间进行处理
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            OutputTag<Tuple2<String, Integer>> late = new OutputTag<Tuple2<String, Integer>>("late"){};

            // 连接socket获取输入的数据
            // nc -lk 8888
            // netstat -natp | grep 8888
            // 测试数据格式, 时间戳 + 数据(窗口是9000-12000, 左闭右开)
            // 10000 hello
            // 11000 gin
            // 11000 hello
            // 12000 hello
            // 15000 hello
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

            SingleOutputStreamOperator<Tuple2<String, Integer>> value = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
                    //如果窗口计算完成后的3秒内, 还出现了应该在计算完成的窗口计算的数据, 那么可以再计算一次
                    //如果 watermark 标记超过3秒了(超过3秒的数据到达会更新), 那么迟到的数据不会被flink历史窗口计算(窗口被清理了)
                    .allowedLateness(Time.seconds(3))
                    //将延迟过于严重的数据输出到侧输出流
                    .sideOutputLateData(late)
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
                    });

            //主输出流, 被处理的数据
            value.print("main");
            //打印侧输出流(可以下沉到其他数据源)
            value.getSideOutput(late).print("late");

            //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
            env.execute("streaming word count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
