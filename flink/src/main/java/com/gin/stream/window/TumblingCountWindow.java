package com.gin.stream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 不使用 keyBy 分流的窗口(all)
 * 应用场景较少(一般用于汇总值/排序, 与key无关)
 *
 * Flink任务Batch是Streaming的一个特例，因此Flink底层引擎是一个流式引擎，
 * 在上面实现了流处理和批处理。而Window就是从Streaming到Batch的桥梁
 * Window窗口就在一个无界流中设置起始位置和终止位置，让无界流变成有界流，
 * 并且在有界流中进行数据处理
 * Window操作常见的业务场景：统计过去一段时间、最近一些元素的数据指标
 *
 * Window窗口分类
 * Window窗口在无界流中设置起始位置和终止位置的方式可以有两种：
 * 根据时间设置
 * 根据窗口数据量（count）设置
 *
 * 根据窗口的类型划分：
 * 滚动窗口
 * 滑动窗口
 *
 * 根据数据流类型划分：
 * Keyed Window：基于分组后的数据流之上做窗口操作
 * Global Window：基于未分组的数据流之上做窗口操作
 *
 * 根据不同的组合方式，可以组合出来8种窗口类型：
 * 1. 基于分组后的数据流上的时间滚动窗口
 * 2. 基于分组后的数据流上的时间滑动窗口
 * 3. 基于分组后的数据流上的count滚动窗口
 * 4. 基于分组后的数据流上的count滑动窗口
 * 5. 基于未分组的数据流上的时间滚动窗口
 * 6. 基于未分组的数据流上的时间滑动窗口
 * 7. 基于未分组的数据流上的count滚动窗口
 * 8. 基于未分组的数据流上的count滑动窗口
 *
 * Time Window（基于时间的窗口）
 * Tumbling Window：滚动窗口，窗口之间没有数据重叠
 * Sliding Window：滑动窗口，窗口内的数据有重叠
 * 在定义滑动窗口的时候，不只是要定义窗口大小，
 * 还要定义窗口的滑动间隔时间（每隔多久滑动一次），
 * 如果滑动间隔时间=窗口大小=滚动窗口
 *
 * 窗口函数定义了针对窗口内元素的计算逻辑，窗口函数大概分为两类：
 * 1. 增量聚合函数，聚合原理：窗口内保存一个中间聚合结果，随着新元素的加入，不断对该值进行更新
 * 这类函数通常非常节省空间 ReduceFunction、AggregateFunction属于增量聚合函数
 * 2. 全量聚合函数，聚合原理：收集窗口内的所有元素，并且在执行的时候对他们进行遍历，
 * 这种聚合函数通常需要占用更多的空间（收集一段时间的数据并且保存），但是它可以支持更复杂的逻辑
 * ProcessWindowFunction、WindowFunction属于全量窗口函数
 * 注意：这两类函数可以组合搭配使用
 *
 * @author gin
 * @date 2021/3/2
 */
public class TumblingCountWindow {

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
                // 因为不是基于keyed stream之上的窗口(统计全局窗口的数据的个数)
                // 所以只需要看窗口中的元素数就可以
                // 不需要看相同的元素的个数
                .countWindowAll(3, 3)
                //如果希望一个window批次中的数据
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> old, Tuple2<String, Integer> current) throws Exception {
                        //这里后面的 word 会被覆盖, 仅仅测试 countWindowAll 用
                        //无实际业务意义
                        old.setField(old.f1 + current.f1, 1);
                        return old;
                    }
                }).print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }


}
