package com.gin.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * 启动 Kafka
 * bin/kafka-server-start.sh -daemon config/server.properties
 * 连接broker-list, 发送消息至kafka名为config的topic
 * cd /opt/software/kafka_2.11-2.0.0/bin
 * ./kafka-console-producer.sh --broker-list node01:9092,node02:9092,node03:9092 --topic config
 * >001 北京
 * 消费测试
 * cd /opt/software/kafka_2.11-2.0.0/bin
 * ./kafka-console-consumer.sh --bootstrap-server node01:9092,node02:9092,node03:9092 --topic config --group group02 --property print.key=true --property print.value=true --property key.separator=,
 *
 *
 * @author gin
 * @date 2021/2/20
 */
public class FlinkKafkaUtils {

    public static void main(String[] args) {

        produceKafkaMsg();

    }

    public static void produceKafkaMsg() {
        MyThreadFactory myThreadFactory = new MyThreadFactory("ts");
        myThreadFactory.newThread(new Runnable() {
            @Override
            public void run() {
                FlinkKafkaUtils.sendPartKafkaMsg();
            }
        }).start();
    }

    public static Properties getKafkaPropsWithSerializer() {
        // 启动 Kafka
        // bin/kafka-server-start.sh -daemon config/server.properties
        // 消费测试
        // ./kafka-console-consumer.sh --bootstrap-server node01:9092,node02:9092,node03:9092 --topic flink-kafka --group group01 --property print.key=true --property print.value=true --property key.separator=,
        //设置连接kafka的配置信息
        Properties props = new Properties();
        //注意: kafka0.10之前版本: zookeeper url, 之后版本: bootstrap.servers
        props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.setProperty("group.id", "flink-kafka-001");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static Properties getKafkaPropsWithDeSerializer() {
        // 启动 Kafka
        // bin/kafka-server-start.sh -daemon config/server.properties
        // 消费测试
        // ./kafka-console-consumer.sh --bootstrap-server node01:9092,node02:9092,node03:9092 --topic flink-kafka --group group01 --property print.key=true --property print.value=true --property key.separator=,
        //设置连接kafka的配置信息
        Properties props = new Properties();
        //注意: kafka0.10之前版本: zookeeper url, 之后版本: bootstrap.servers
        props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.setProperty("group.id", "flink-kafka-001");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static Properties getKafkaProps() {
        // 启动 Kafka
        // bin/kafka-server-start.sh -daemon config/server.properties
        // 消费测试
        // ./kafka-console-consumer.sh --bootstrap-server node01:9092,node02:9092,node03:9092 --topic flink-kafka --group group01 --property print.key=true --property print.value=true --property key.separator=,
        //设置连接kafka的配置信息
        Properties props = new Properties();
        //注意: kafka0.10之前版本: zookeeper url, 之后版本: bootstrap.servers
        props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.setProperty("group.id", "flink-kafka-001");
        return props;
    }

    private static void sendPartKafkaMsg() {
        //配置连接kafka的信息
        Properties props = getKafkaPropsWithSerializer();

        //创建一个kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        //消费本地高速路卡口监控车辆速度数据 向Kafka发送消息
        List<String> strList = FileUtils.toListByRandomAccessFile("./data/carFlow_all_column_test.txt");
        int batchCount = 100;
        for (int i = 0; i < batchCount; i++) {
            for (String elem : strList) {
                //打印读取到的数据 System.out.println(elem)
                //kv mq 获取部分数据
                String[] splits = elem.split(",");
                //卡口号
                String monitorId = splits[0].replace("'", "");
                //车牌号
                String carId = splits[2].replace("'", "");
                //时间
                String timestamp = splits[4].replace("'", "");
                //速度
                String speed = splits[6];
                StringBuilder builder = new StringBuilder();
                StringBuilder info = builder.append(monitorId).append("\t").append(carId).append("\t").append(timestamp).append("\t").append(speed);
                kafkaProducer.send(new ProducerRecord<String, String>("flink-kafka", i + "", info.toString()));
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
