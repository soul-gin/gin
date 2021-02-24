package com.gin.stream.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;


/**
 * @author gin
 * @date 2021/2/22
 * connect 合并两个数据流并且保留两个数据流的数据类型，能够共享两个流的状态
 * 假合并, 可以合并不同类型的流
 * 合并后处理时是每个流单独处理
 */
public class ConnectOperator {

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //同类型的 DataStream 才能合并
            DataStreamSource<Integer> stream1 = env.fromElements(7, 8, 9);
            DataStreamSource<Long> stream2 = env.generateSequence(1, 3);
            ConnectedStreams<Integer, Long> connect = stream1.connect(stream2);
            useMap(connect);
            useFlatMap(connect);
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void useFlatMap(ConnectedStreams<Integer, Long> connect) {
        connect
                .flatMap(new CoFlatMapFunction<Integer, Long, Integer>() {
                    @Override
                    public void flatMap1(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void flatMap2(Long value, Collector<Integer> out) throws Exception {
                        out.collect(value.intValue());
                    }
                })
                .print().setParallelism(1);
    }

    private static void useMap(ConnectedStreams<Integer, Long> connect) {
        //需要将流真正处理成同类型后, 才能打印
        connect
                .map(new CoMapFunction<Integer, Long, Integer>() {
                    @Override
                    public Integer map1(Integer value) {
                        return value;
                    }

                    @Override
                    public Integer map2(Long value) {
                        return value.intValue();
                    }
                })
                .print();
        //执行任务
    }

}
