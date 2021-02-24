package com.gin.stream.state;

import com.gin.common.CommonConstants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author gin
 * @date 2021/2/24
 */
public class ValueStateTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // netstat -natp |grep 8888
        // nc -lk 8888
        // 连接socket获取输入的数据
        // 场景, 根据车牌号(key)判断车速变化是否过快(急加速)
        // 数据格式:
        /*
        car1 100
        car1 150
        car2 100
        car2 120
        */
        DataStreamSource<String> text = env.socketTextStream("node01", 8888, "\n");

        // 注意 RichMapFunction 才有生命周期管理, 可以使用 state
        // MapFunction 无生命周期管理, 仅能做数据的一一映射
        text
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String res) {
                        String[] split = res.split(CommonConstants.STR_BLANK);
                        return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
                    }
                })
                //根据第一个字段(车牌号)分组
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, String>() {

                    // 这里不适合使用本地变量来保存历史值, 而应该使用状态变量
                    // 状态变量会持久化到外部存储中, 下次flink重启也可以保留
                    ValueState<Integer> latestSpeedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 定义 state 根据key存储一个value
                        // 指定存储状态名称, 指定存储类型
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("latestSpeed",
                                TypeInformation.of(new TypeHint<Integer>() {
                                }));
                        // 注册 state
                        latestSpeedState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public String map(Tuple2<String, Integer> carInfo) {
                        String result = null;
                        try {
                            //获取状态历史值
                            Integer lastSpeed = latestSpeedState.value();
                            //更新状态值
                            latestSpeedState.update(carInfo.f1);
                            //变化超过30定义为超速
                            if (lastSpeed != null && carInfo.f1 - lastSpeed > 30) {
                                result = carInfo.f0 + " over speed: " + carInfo.f1;
                            } else {
                                result = carInfo.f0;
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return result;
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
