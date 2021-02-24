package com.gin.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author gin
 * @date 2021/2/20
 */
public class CustomSourceStandalone {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //[String] 发射数据类型

            //Source: 1 is not a parallel source
            DataStreamSource<String> stream = env
                    //基于 SourceFunction 接口实现自定义数据源，这个只支持单并行度
                    .addSource(new SourceFunction<String>() {
                        Boolean flag = true;

                        @Override
                        public void run(SourceContext<String> ctx) throws Exception {
                            // run  读取任何地方数据，然后将数据发射出去
                            // 例如: 在run方法中读取 redis 数据
                            Random random = new Random();
                            while (flag) {
                                ctx.collect("hello" + random.nextInt(1000));
                                Thread.sleep(500);
                            }
                        }

                        @Override
                        public void cancel() {
                            flag = false;
                        }
                    })
                    //注意 SourceFunction 并行度只能为1;
                    .setParallelism(1);
            stream.print().setParallelism(1);
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
