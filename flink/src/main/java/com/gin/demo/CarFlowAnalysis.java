package com.gin.demo;

import com.gin.common.FlinkKafkaUtils;
import com.gin.stream.source.ReadKafka;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gin
 * @date 2021/2/22
 * <p>
 * 分析 各个卡口车流量
 */
public class CarFlowAnalysis {

    public static void main(String[] args) {
        //发送Kafka消息
        FlinkKafkaUtils.produceKafkaMsg();

        //消费消息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据流
        //计算每个卡口号(monitorID) 通过车的数量
        DataStream<Tuple2<String, String>> stream = ReadKafka.getKafkaTuple2DataStream(env);

        stream
                //剔除key
                .map(new MapFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String map(Tuple2<String, String> value) throws Exception {
                        return value.f1;
                    }
                })
                // stream 中String元素类型, 变成二元组类型(映射成:卡口,1)
                // k:monitorId v:1
                //如果需要统计每分钟下每个卡口的流量
                //那么可以设置key为组合key: monitorId + 年月日时分
                //然后继续统计
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {

                        String monitorId = value.split("\t")[0];
                        return new Tuple2<>(monitorId, 1);
                    }
                })
                /*
                 * 相同key的 数据一定是由某一个subtask处理
                 * 一个subtask 可能会处理多个key所对应的数据
                 * Tuple2<String, Integer> 的第一个元素下标为 0
                 */
                .keyBy(0)
                /*.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> old, Tuple2<String, Integer> current) {
                        //old：上次聚合的结果  current：本次要聚合的数据
                        old.setField(old.f1 + current.f1, 1);
                        return old;
                    }
                })*/
                //上面的reduce可以直接被 Aggregations 聚合算子中的sum替换
                //Aggregations 聚合算子包含: sum, min, max, minBy, maxBy
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
