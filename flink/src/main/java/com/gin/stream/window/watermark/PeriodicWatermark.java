package com.gin.stream.window.watermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * 自定义水印实现(使用 ProcessTime 则不需要 Watermark , ProcessTime不关心数据是否乱序 )
 *
 * 没有Watermark, 只使用 EventTime:
 * 窗口按 EventTime 计算后, 划分每个窗口开始,结束时间点; 按读取到消息的eventTime判断是否超过结束时间点触发统计
 * 有了Watermark:
 * 窗口按 EventTime 计算后, 划分每个窗口开始,结束时间点; 按读取到消息被标记上的Watermark判断是否超过结束时间点触发统计
 * Watermark 标记规则: 例: 窗口结束时间-2秒 -> 允许消息延迟2秒
 * <p>
 * Watermark(水印)本质就是时间戳
 * 在使用Flink处理数据的时候，数据通常都是按照事件产生的时间（事件时间）的顺序进入到Flink，
 * 但是在遇到特殊情况下，比如遇到网络延迟或者使用Kafka（多分区）很难保证数据都是按照
 * 事件时间的顺序进入Flink，很有可能是乱序进入。
 * 如果使用的是事件时间这个语义，数据一旦是乱序进入，那么在使用Window处理数据的时候，
 * 就会出现延迟数据不会被计算的问题
 * 举例： Window窗口长度10s，滚动窗口
 * 001 zs 2020-04-25 10:00:01
 * 001 zs 2020-04-25 10:00:02
 * 001 zs 2020-04-25 10:00:03
 * 001 zs 2020-04-25 10:00:11 窗口触发执行
 * 001 zs 2020-04-25 10:00:05 延迟数据，不会被上一个窗口所计算导致计算结果不正确
 * Watermark+Window 可以很好的解决延迟数据的问题(处理允许延时时间范围内的数据)
 * Flink窗口计算的过程中，如果数据全部到达就会到窗口中的数据做处理，如果过有延迟数据，
 * 那么窗口需要等待指定范围内的数据到来之后，再触发窗口执行，需要等待多久？
 * 不可能无限期等待，我们用户可以自己来设置延迟时间(业务允许延迟时间范围)
 * 这样就可以尽可能保证延迟数据被处理
 * 根据用户指定的延迟时间生成水印（Watermak = 最大事件时间-指定延迟时间），
 * 当Watermak 大于等于窗口的停止时间，这个窗口就会被触发执行
 * 举例：Window窗口长度10s(01-10)，滚动窗口，指定延迟时间3s
 * 001 ls 2020-04-25 10:00:01 wm:2020-04-25 09:59:58
 * env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) val stream = env.socketTextStream("node01", 8888).assignAscendingTimestamps(data => { val splits = data.split(" ") splits(0).toLong })stream .flatMap(x=>x.split(" ").tail) .map((_, 1)) .keyBy(_._1) // .timeWindow(Time.seconds(10)) .window(TumblingEventTimeWindows.of(Time.seconds(10))) .reduce((v1: (String, Int), v2: (String, Int)) => { (v1._1, v1._2 + v2._2) }).print() env.execute() } }
 * 001 ls 2020-04-25 10:00:02 wm:2020-04-25 09:59:59
 * 001 ls 2020-04-25 10:00:03 wm:2020-04-25 10:00:00
 * 001 ls 2020-04-25 10:00:09 wm:2020-04-25 10:00:06
 * 001 ls 2020-04-25 10:00:12 wm:2020-04-25 10:00:09
 * 001 ls 2020-04-25 10:00:08 wm:2020-04-25 10:00:05 延迟数据
 * 001 ls 2020-04-25 10:00:13 wm:2020-04-25 10:00:10 此时wm >= window end time 触发窗口
 * 执行 处理的是事件时间01-10的数据，并不是水印时间为01-10的数据 重点
 * 讲道理，如果没有Watermark在倒数第三条数据(10:00:12)来的时候，就会触发执行，
 * 那么倒数第二条的延迟数据就不会被计算，那么有了水印可以处理延迟3s内的数据(10:00:08 这条)
 * 注意：如果数据不会乱序进入Flink，没必要使用Watermark
 *
 * @author gin
 * @date 2021/3/3
 */
public class PeriodicWatermark {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //每隔100ms(默认值)向数据流中插入一个 Watermark
        env.getConfig().setAutoWatermarkInterval(100);
        //2、在往socket发射数据的时候 必须携带时间戳
        long delay = 3000L;
        //根据数据时间进行处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* 连接socket获取输入的数据
        // nc -lk 8888
        // netstat -natp | grep 8888
        // 测试数据格式, 时间戳 + 数据(窗口是9000-12000, 左闭右开)
10000 hello gin
11000 hello soul
12000 hello soul
10000 hello gin
15000 hello soul
        */
        SingleOutputStreamOperator<String> stream = env.socketTextStream("node01", 8888)
                .assignTimestampsAndWatermarks(
                        //自己实现 AssignerWithPeriodicWatermarks
                        new AssignerWithPeriodicWatermarks<String>() {

                            long maxTime = 0L;

                            @Override
                            public long extractTimestamp(String element, long previousElementTimestamp) {
                                //事件时间
                                long currentTime = Long.parseLong(element.split(" ")[0]);
                                //currentTime可能是延迟消息时间, 避免 maxTime 时间回流
                                maxTime = Math.max(maxTime, currentTime);
                                //返回当前事件的时间(eventTime)
                                return currentTime;
                            }

                            @Nullable
                            @Override
                            public Watermark getCurrentWatermark() {
                                //返回当前事件的 Watermark
                                return new Watermark(maxTime - delay);
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
