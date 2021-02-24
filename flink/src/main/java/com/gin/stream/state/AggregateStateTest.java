package com.gin.stream.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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
public class AggregateStateTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //统计每个用户交易金额
        //Aggregate 在并行度上高于 reduce; 如果cu够多, Aggregate可以提升计算效率
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
                    AggregatingState<Long, Long> userAmountReduceState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 定义 ListState 根据key存储list数据
                        // 指定存储状态名称, 指定存储类型
                        // <IN, ACC, OUT> IN-输出类型; ACC-聚合类型; OUT-输出类型
                        AggregatingStateDescriptor<Long, Long, Long> stateDescriptor = new AggregatingStateDescriptor<>("agg",
                                new AggregateFunction<Long, Long, Long>() {

                                    //初始化累加器
                                    @Override
                                    public Long createAccumulator() {
                                        //单个subtask的累加器初始值为0
                                        return 0L;
                                    }

                                    //往累加器中累加值
                                    @Override
                                    public Long add(Long value, Long accumulator) {
                                        //每来一条数据会计算一次
                                        //历史值加上当前值
                                        return value + accumulator;
                                    }

                                    //返回最终结果
                                    @Override
                                    public Long getResult(Long accumulator) {
                                        //直接返回当前subtask累加器计算的结果, 不做其他特殊处理
                                        return accumulator;
                                    }

                                    //合并两个累加器值
                                    @Override
                                    public Long merge(Long accumulatorA, Long accumulatorB) {
                                        //因累加器可以分配到多个subtask进行并行处理, 提高处理速度
                                        //所以最后需要对并行计算的累加器计算结果进行聚合操作
                                        return accumulatorA + accumulatorB;
                                    }
                                },
                                TypeInformation.of(new TypeHint<Long>() {
                                }));
                        // 注册 state
                        userAmountReduceState = getRuntimeContext().getAggregatingState(stateDescriptor);
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
