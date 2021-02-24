package com.gin.stream.sink;

import com.gin.common.FlinkKafkaUtils;
import com.gin.stream.WordCountStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author gin
 * @date 2021/2/23
 */
public class KafkaSinkTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // word count 单词统计数据, 通过 RichMapFunction 算子来写入redis
        // nc -lk 8888
        DataStream<Tuple2<String, Integer>> wordCountStream = WordCountStream.getWordCountStream(env);

        // 启动 Kafka
        // bin/kafka-server-start.sh -daemon config/server.properties
        // 消费测试
        // ./kafka-console-consumer.sh --bootstrap-server node01:9092,node02:9092,node03:9092 --topic flink-kafka --group group01 --property print.key=true --property print.value=true --property key.separator=,
        //设置连接kafka的配置信息
        Properties props = FlinkKafkaUtils.getKafkaProps();

        //sink
        wordCountStream.addSink(new FlinkKafkaProducer<Tuple2<String, Integer>>("flink-kafka",
                new KafkaSerializationSchema<Tuple2<String, Integer>>() {

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> value, @Nullable Long time) {
                        return new ProducerRecord<byte[], byte[]>("flink-kafka", value.f0.getBytes(), String.valueOf(value.f1).getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
        );

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
