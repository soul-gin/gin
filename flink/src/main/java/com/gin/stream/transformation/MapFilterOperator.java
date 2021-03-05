package com.gin.stream.transformation;

import com.gin.common.CommonConstants;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author gin
 * @date 2021/2/20
 * 算子 <-> 函数
 */
public class MapFilterOperator {


    public static void main(String[] args) {
        try {
            // 获取运行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // 连接socket获取输入的数据
            // nc -lk 8888
            // netstat -natp | grep 8888
            DataStreamSource<String> stream = env.socketTextStream("node01", 8888, "\n");

            // 1.通过 flatMap 实现数据过滤
            //useFlatMapFilter(stream, env);

            // 2.通过 filter 直接数据过滤
            //useFilter(stream, env);

            // 3.测试 keyBy reduce 根据key分组统计
            useKeyBy(stream, env);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void useFlatMapFilter(DataStreamSource<String> stream, StreamExecutionEnvironment env) throws Exception {
        //如何使用flatMap 代替 filter
        //数据中包含了abc 那么这条数据就过滤掉  flatMap
        //flatMap算子：map flat(扁平化)   flatMap 集合
        SingleOutputStreamOperator<String> flatMap = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String resourceStr, Collector<String> collector) throws Exception {
                //排除包含 0 的数据
                if (!resourceStr.contains(CommonConstants.STR_ZERO)) {
                    collector.collect(resourceStr);
                }
            }
        });
        flatMap.print();
        env.execute();
    }

    private static void useFilter(DataStreamSource<String> stream, StreamExecutionEnvironment env) throws Exception {
        //如何使用flatMap 代替 filter
        //数据中包含了abc 那么这条数据就过滤掉  filterMap
        //flatMap算子：map flat(扁平化)   filterMap 集合
        SingleOutputStreamOperator<String> filterMap = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String resourceStr) throws Exception {
                // 过滤掉包含 0 的数据
                // false 表示过滤掉
                return !resourceStr.contains(CommonConstants.STR_ZERO);
            }
        });
        filterMap.print();
        env.execute();
    }

    private static void useKeyBy(DataStreamSource<String> stream, StreamExecutionEnvironment env) throws Exception {
        //flatMap 结合 map 理念
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String resourceStr, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //flatMap 处理
                for (String mapKey : resourceStr.split(CommonConstants.STR_BLANK)) {
                    //map 处理
                    collector.collect(new Tuple2<String, Integer>(mapKey, 1));
                }
            }
        });

        mapStream
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> old, Tuple2<String, Integer> current) throws Exception {
                        System.out.println("oldValue=" + old.f1 + "  currentValue=" + current.f1);
                        old.setField(old.f1 + current.f1, 1);
                        return old;
                    }
                })
                .print();
        env.execute();
    }
}
