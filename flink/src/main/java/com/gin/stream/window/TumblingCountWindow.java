package com.gin.stream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 不使用 keyBy 分流的窗口(all)
 * 应用场景较少(一般用于汇总值/排序, 与key无关)
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
