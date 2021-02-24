package com.gin.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author gin
 * @date 2021/2/20
 */
public class ReadCollections {

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //一般测试用
            DataStreamSource<Integer> stream1 = env.fromElements(7, 8, 9);
            DataStreamSource<Integer> stream2 = env.fromCollection(Arrays.asList(1, 2, 3));
            //打印
            stream1.print();
            stream2.print();
            //执行任务
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
