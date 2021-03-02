package com.gin.demo;

import com.gin.common.FlinkKafkaUtils;
import com.gin.stream.source.ReadKafka;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 1. 增量聚合函数, 聚合原理：窗口内保存一个中间聚合结果, 随着新元素的加入, 不断对该值进行更新
 * 这类函数通常非常节省空间 ReduceFunction、AggregateFunction属于增量聚合函数
 * 2. 全量聚合函数, 聚合原理: 收集窗口内的所有元素, 并且在执行的时候对他们进行遍历
 * 这种聚合函数通常需要占用更多的空间（收集一段时间的数据并且保存）
 * 但是它可以支持更复杂的逻辑, ProcessWindowFunction、WindowFunction属于全量窗口函数
 * <p>
 * 全量聚合函数: 一般用于汇总值/排序, 与key无关
 * 每隔10s对窗口内所有汽车的车速进行排序
 * 每隔10s统计出窗口内所有车辆的最大及最小速度
 *
 * @author gin
 * @date 2021/3/2
 */
public class SlidingTimeWindow {

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
                //每10秒统计一次所有车30分钟内的平均速度
                .timeWindowAll(Time.minutes(30), Time.seconds(10))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
                    /**
                     * 全量聚合函数
                     * @param context 上下文对象
                     * @param elements 窗口中的所有元素
                     * @param out 输出结果
                     * @throws Exception 计算异常
                     */
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

                        HashMap<String, Integer> map = new HashMap<>();
                        elements.forEach((val) -> {
                            map.put(val.f0, val.f1);
                        });
                        List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
                        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                            @Override
                            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                                // 按照value值，用compareTo()方法默认是从小到大排序
                                //return o1.getValue().compareTo(o2.getValue());
                                // 按照value值，重小到大排序
                                //return o1.getValue() - o2.getValue();
                                // 按照value值，从大到小排序
                                return o2.getValue() - o1.getValue();
                            }
                        });
                        //车速最高的 车速最低的
                        System.out.println(list.get(0).getKey() + " : " + list.get(0).getValue() + " " + list.get(list.size() - 1).getKey() + " : " + list.get(list.size() - 1).getValue());
                        //输出车速最高的车牌号
                        out.collect(list.get(0).getKey());
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
