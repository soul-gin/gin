package com.gin.stream.sink;

import com.gin.stream.WordCountStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author gin
 * @date 2021/2/23
 *
 * 将WordCount的计算结果写入到Redis中
 * 注意：Flink是一个流式计算框架
 * 会出现需要插入: hello 1;  hello 3;
 * 最终需要插入的是: hello 3
 * redis存数据的需要是幂等操作
 * redis: hset（word，int）
 *
 */
public class RedisSinkTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // word count 单词统计数据, 通过 RichMapFunction 算子来写入redis
        // nc -lk 8888
        DataStream<Tuple2<String, Integer>> wordCountStream = WordCountStream.getWordCountStream(env);

        // redis-cli -h node01 -p 6379
        // select 3
        // keys *
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("node01").setPort(6379).setDatabase(3).build();

        wordCountStream.addSink(new RedisSink<Tuple2<String, Integer>>(jedisPoolConfig, new RedisMapper<Tuple2<String, Integer>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                //指定操作Redis的命令
                //使用 hset 命令,
                return new RedisCommandDescription(RedisCommand.HSET,"wc");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> value) {
                return value.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> value) {
                return String.valueOf(value.f1);
            }
        }));

        /*
        测试:

        统计
        nc -lk 8888
        hello hello word

        查询
        redis-cli -h node01 -p 6379
        select 3
        node01:6379[3]> hget wc word
        "1"
        node01:6379[3]> hget wc hello
        "2"

         */
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
