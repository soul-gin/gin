package com.gin.stream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * 会话窗口
 * 场景较少
 *
 * @author gin
 * @date 2021/3/2
 */
public class SessionTimeWindow {

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
                // SessionWindows 在时间限定范围内窗口没有输入, 那么就会触发计算
                // EventTime  事件时间(元素在数据源中产生的时间), 数据产生的时间
                // IngestionTime 元素进入flink source的系统时间, 数据被source function处理的时间
                // ProcessTime  元素被处理的系统时间(元素进入窗口时间), 数据被 transformation 处理的时间(flink 默认时间语义)
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        elements.forEach(val -> {
                            out.collect(val.f0 + " " + val.f1);
                        });
                    }
                })
                .print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }

}
