package com.gin.demo;

import com.gin.common.FlinkKafkaUtils;
import com.gin.stream.source.ReadKafka;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 读取kafka数据，统计每隔10s最近30分钟内，每辆车的平均速度
 *
 * @author gin
 * @date 2021/3/2
 */
public class SlidingTimeWindowKeyed {

    public static void main(String[] args) {
        //发送Kafka消息
        FlinkKafkaUtils.produceKafkaMsg();
        //消费消息, 数据来源: FlinkKafkaUtils.sendPartKafkaMsg()
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ReadKafka.getKafkaTuple2DataStream(env)
                //剔除key
                .map(new MapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String map(Tuple2<String, String> value) throws Exception {
                        return value.f1;
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {

                        String carId = value.split("\t")[1];
                        Integer speed = Integer.parseInt(value.split("\t")[3]);
                        return new Tuple2<>(carId, speed);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                //每10秒统计一次所有车30分钟内的平均速度
                .timeWindow(Time.minutes(30), Time.seconds(10))
        .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple3<String, Integer, Integer> createAccumulator() {
                //初始化(创建)累加器
                //车牌号, 速度总和, 经过卡口数
                return new Tuple3<>("", 0, 0);
            }

            @Override
            public Tuple3<String, Integer, Integer> add(Tuple2<String, Integer> val, Tuple3<String, Integer, Integer> acc) {
                //累加器进行累加操作(累加器 + 数据源)
                //车牌号, 速度累加, 经过卡口数+1
                return new Tuple3<>(val.f0, val.f1 + acc.f1, acc.f2 + 1);
            }

            @Override
            public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> acc1, Tuple3<String, Integer, Integer> acc2) {
                //多个累加器的结果进行聚合(累加器 + 累加器)
                return new Tuple3<>(acc1.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2);
            }

            @Override
            public Tuple2<String, Double> getResult(Tuple3<String, Integer, Integer> acc) {
                //返回结果
                //车牌号, 速度总和/卡口数
                return new Tuple2<>(acc.f0, acc.f1 * 1.0/acc.f2);
            }


        }).print()
        ;

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
