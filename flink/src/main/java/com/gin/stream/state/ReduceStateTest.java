package com.gin.stream.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author gin
 * @date 2021/2/24
 */
public class ReduceStateTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //统计每个用户交易金额
        DataStreamSource<Tuple2<String, Long>> tuple2Stream = env.fromCollection(Arrays.asList(
                new Tuple2<String, Long>("luffy", 500L),
                new Tuple2<String, Long>("luffy", 1000L),
                new Tuple2<String, Long>("luffy", 600L),
                new Tuple2<String, Long>("nier", 100L),
                new Tuple2<String, Long>("nier", 200L),
                new Tuple2<String, Long>("nier", 300L)
        ));

        tuple2Stream
                //根据第一个字段进行分组
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {

                    // 这里不适合使用本地变量来保存历史值, 而应该使用状态变量
                    // 状态变量会持久化到外部存储中, 下次flink重启也可以保留
                    ReducingState<Long> userAmountReduceState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 定义 ListState 根据key存储list数据
                        // 指定存储状态名称, 指定存储类型
                        ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<>("reduce",
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long v1, Long v2) throws Exception {
                                        //历史值与当前值求和
                                        return v1 + v2;
                                    }
                                },
                                TypeInformation.of(new TypeHint<Long>() {
                                }));
                        // 注册 state
                        userAmountReduceState = getRuntimeContext().getReducingState(stateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        String name = value.f0;
                        Long amount = value.f1;
                        //累计历史值与当前值
                        userAmountReduceState.add(amount);
                        //输出累计后的值
                        return new Tuple2<>(name, userAmountReduceState.get());
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
