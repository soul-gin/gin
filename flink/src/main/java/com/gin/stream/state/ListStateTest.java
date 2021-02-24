package com.gin.stream.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author gin
 * @date 2021/2/24
 */
public class ListStateTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //统计每辆车的运行轨迹
        //所谓运行轨迹就是这辆车的信息 按照时间排序，卡口号串联起来
        //卡口  车牌  事件时间  车速
        DataStreamSource<Tuple4<String, String, Long, Long>> tuple4Stream = env.fromCollection(Arrays.asList(
                new Tuple4<String, String, Long, Long>("310999017401", "沪M82608", 1614135561000L, 100L),
                new Tuple4<String, String, Long, Long>("310999017402", "沪M82608", 1614135672000L, 120L),
                new Tuple4<String, String, Long, Long>("310999017403", "沪M82608", 1614135783000L, 130L),
                new Tuple4<String, String, Long, Long>("310999017401", "沪M82607", 1614135561000L, 130L),
                new Tuple4<String, String, Long, Long>("310999017402", "沪M82607", 1614135662000L, 130L),
                new Tuple4<String, String, Long, Long>("310999017403", "沪M82607", 1614135763000L, 130L)
        ));

        tuple4Stream
                .keyBy(1)
                .map(new RichMapFunction<Tuple4<String, String, Long, Long>, Tuple2<String, String>>() {

            // 这里不适合使用本地变量来保存历史值, 而应该使用状态变量
            // 状态变量会持久化到外部存储中, 下次flink重启也可以保留
            ListState<Tuple2<String, Long>> monitorIdAndTimeList;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 定义 ListState 根据key存储list数据
                // 指定存储状态名称, 指定存储类型
                ListStateDescriptor<Tuple2<String, Long>> stateDescriptor = new ListStateDescriptor<>("speedList",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }));
                // 注册 state
                monitorIdAndTimeList = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public Tuple2<String, String> map(Tuple4<String, String, Long, Long> value) throws Exception {
                String carId = value.f1;
                StringBuilder trace = new StringBuilder("\n");
                monitorIdAndTimeList.add(new Tuple2<>(value.f0, value.f3));
                Iterable<Tuple2<String, Long>> monitorIdAndTimeIter = monitorIdAndTimeList.get();

                if (null != monitorIdAndTimeIter) {
                    for (Tuple2<String, Long> next : monitorIdAndTimeIter) {
                        trace.append("通过:").append(next.f0).append(" 时间:").append(next.f1).append("\n");
                    }
                }
                return new Tuple2<>(carId, trace.toString());
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
