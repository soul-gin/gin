package com.gin.conntable;

import com.gin.common.FlinkKafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 对于维度表更新频率高并且对于查询维度表的实时性要求高
 * 实现方案：管理员在修改配置文件的时候，需要将更改的信息同步值Kafka配置Topic中，
 * 然后将kafka的配置流信息变成广播流，广播到业务流的各个线程中
 *
 * @author gin
 * @date 2021/3/9
 */
public class BroadCastStream {

    public static void main(String[] args) {
        try {
            //测试:
            // netstat -natp |grep 8888
            // nc -lk 8888
            // 连接broker-list, 发送消息至kafka名为config的topic
            // ./kafka-console-producer.sh --broker-list node01:9092,node02:9092,node03:9092 --topic config
            // >001 北京
            // >002 上海
            // >003 深圳
            // >004 杭州
            // 再向8888端口输入测试数据
            // 001
            // 002
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //kafka 广播流
            Properties propDeSerializer = FlinkKafkaUtils.getKafkaPropsWithDeSerializer();
            FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("config", new SimpleStringSchema(), propDeSerializer);
            //从topic最开始的数据读取
            //consumer.setStartFromEarliest()
            //从最新的数据开始读取
            consumer.setStartFromLatest();
            //动态配置信息流
            DataStreamSource<String> configureStream = env.addSource(consumer);

            //定义map state描述器
            MapStateDescriptor<String,  String> descriptor = new MapStateDescriptor<String,  String>("dynamicConfig",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            //设置广播流的数据描述信息
            BroadcastStream<String> broadcastStream = configureStream.broadcast(descriptor);

            //业务流
            DataStreamSource<String> busStream = env.socketTextStream("node01", 8888);
            //connect关联业务流与配置信息流，broadcastStream流中的数据会广播到下游的各个线程中
            busStream.connect(broadcastStream)
                    .process(new BroadcastProcessFunction<String, String, String>() {
                        //每来一个新的元素都会调用一下这个方法
                        @Override
                        public void processElement(String line, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                            //获取kafka中缓存的状态数据(MapStateDescriptor)
                            ReadOnlyBroadcastState<String, String> broadcast = ctx.getBroadcastState(descriptor);
                            //根据业务数据(id: 001 002...)去MapStateDescriptor中查询是否有匹配的中文描述
                            String city = broadcast.get(line);
                            //配置信息判断
                            if(StringUtils.isEmpty(city)){
                                //未查询到, 输出 not found city
                                out.collect("not found city");
                            }else{
                                //匹配中, 收集输出
                                out.collect(city);
                            }
                        }

                        //kafka中配置流信息，写入到广播流中
                        @Override
                        public void processBroadcastElement(String line, Context ctx, Collector<String> out) throws Exception {
                            BroadcastState<String, String> broadcast = ctx.getBroadcastState(descriptor);
                            //kafka中的数据切割, 存入MapStateDescriptor
                            String[] elems = line.split(" ");
                            //设置key为id(001 002...), value为中文描述(北京 上海...)
                            broadcast.put(elems[0], elems[1]);
                        }
                    }).print();

            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
