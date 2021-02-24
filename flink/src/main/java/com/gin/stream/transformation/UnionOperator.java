package com.gin.stream.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;


/**
 * @author gin
 * @date 2021/2/22
 * 必须要同类型的 DataStream 才能合并
 */
public class UnionOperator {

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //同类型的 DataStream 才能合并
            DataStreamSource<Integer> stream1 = env.fromElements(7, 8, 9);
            DataStreamSource<Integer> stream2 = env.fromCollection(Arrays.asList(1, 2, 3));
            DataStream<Integer> unionStream = stream1.union(stream2);
            //打印
            unionStream.print().setParallelism(1);
            //执行任务
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
