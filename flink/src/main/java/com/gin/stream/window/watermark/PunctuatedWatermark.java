package com.gin.stream.window.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * 间歇水印
 * 在观察到事件后，会依据用户指定的条件来决定是否发射水印
 * 比如，在车流量的数据中，001卡口通信经常异常，传回到服务器的数据会有延迟问题，
 * 其他的卡口都是正常的，那么这个卡口的数据需要打上水印
 *
 * @author gin
 * @date 2021/3/3
 */
public class PunctuatedWatermark {

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
10000 001
11000 001
12000 002
10000 002
16000 003
16000 001
        */
        SingleOutputStreamOperator<String> stream = env.socketTextStream("node01", 8888)
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<String>() {

                            long maxTime = 0L;

                            @Override
                            public long extractTimestamp(String lastElement, long previousElementTimestamp) {
                                //事件时间
                                long currentTime = Long.parseLong(lastElement.split(" ")[0]);
                                //currentTime可能是延迟消息时间, 避免 maxTime 时间回流
                                maxTime = Math.max(maxTime, currentTime);
                                //返回当前事件的时间(eventTime)
                                return currentTime;
                            }

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(String element, long extractedTimestamp) {
                                //场景: 某些数据源设备延迟严重, 其他设备不会延迟
                                //第二位, 字段值为001(标志)的数据可以延迟3秒
                                if ("001".equals(element.split(" ")[1])) {
                                    //对应标志位才会生成水印
                                    return new Watermark(maxTime - 3000);
                                } else {
                                    //其他标志位不会生成水印, 也就不会触发计算
                                    return null;
                                }
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
