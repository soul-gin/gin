package com.gin.stream.transformation;

import com.gin.stream.WordCountStream;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.Jedis;

/**
 * @author gin
 * @date 2021/2/22
 */
public class RichMapFunctionOperator {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // word count 单词统计数据, 通过 RichMapFunction 算子来写入redis
        // nc -lk 8888
        DataStream<Tuple2<String, Integer>> wordCountStream = WordCountStream.getWordCountStream(env);

        wordCountStream.print();
        wordCountStream.map(new RichMapFunction<Tuple2<String, Integer>, String>() {

            private Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                //Task的名字
                String taskName = getRuntimeContext().getTaskName();
                //获取子任务的名字
                String subtaskName = getRuntimeContext().getTaskNameWithSubtasks();
                System.out.println("taskName:" + taskName + "\t subtaskName:" + subtaskName);

                //在subtask启动的时候，首先调用的方法 redis 3号数据库( select 3 )
                jedis = new Jedis("node01", 6379);
                // 注意同时打开 word count 端口, 避免 connect time out
                // nc -lk 8888
                // 测试用: System.out.println(jedis.get("1"))
                // 对应 redis 需要选择对应库(命令: select 3 )进行 keys * 查询
                jedis.select(3);

            }

            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                try {
                    jedis.set(value.f0, String.valueOf(value.f1));
                    System.out.println("key=" + value.f0 + " value=" + jedis.get(value.f0));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return value.f0;
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        });

        try {
            env.execute("store redis from word count");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
