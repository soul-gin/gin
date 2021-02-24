package com.gin.stream.source;

import com.gin.common.FlinkKafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

/**
 * @author gin
 * @date 2021/2/20
 */
public class ReadKafkaWithoutKey {

    public static void main(String[] args) {
        try {
            //发送Kafka消息
            FlinkKafkaUtils.produceKafkaMsg();

            //消费消息
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Properties kafkaProps = FlinkKafkaUtils.getKafkaPropsWithSerializer();

            //只获取 value
            DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<String>("flink-kafka", new SimpleStringSchema(), kafkaProps));

            stream.print();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
