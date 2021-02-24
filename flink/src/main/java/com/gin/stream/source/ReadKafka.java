package com.gin.stream.source;

import com.gin.common.FlinkKafkaUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;


/**
 * @author gin
 * @date 2021/2/20
 */
public class ReadKafka {

    public static void main(String[] args) {
        try {
            //发送Kafka消息
            FlinkKafkaUtils.produceKafkaMsg();

            //消费消息
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<Tuple2<String, String>> stream = getKafkaTuple2DataStream(env);
            stream.print();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static DataStream<Tuple2<String, String>> getKafkaTuple2DataStream(StreamExecutionEnvironment env) {
        Properties kafkaProps = FlinkKafkaUtils.getKafkaPropsWithSerializer();

        //获取 key 和 value
        return env.addSource(new FlinkKafkaConsumer<Tuple2<String, String>>("flink-kafka", new KafkaDeserializationSchema<Tuple2<String, String>>() {
            @Override
            public TypeInformation<Tuple2<String, String>> getProducedType() {
                //指定一下返回的数据类型  Flink提供的类型
                //可选强转(TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class)
                return TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
            }

            @Override
            public boolean isEndOfStream(Tuple2<String, String> s) {
                //判断结束
                return false;
            }

            @Override
            public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                //要进行序列化的字节流
                String key = new String(consumerRecord.key(), StandardCharsets.UTF_8);
                String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);
                return new Tuple2<String, String>(key, value);
            }
        }, kafkaProps));
    }


}
