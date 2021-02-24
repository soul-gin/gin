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
 * 将WordCount的计算结果写入到Mysql中
 * 注意：Flink是一个流式计算框架
 * 会出现需要插入: hello 1;  hello 3;
 * 最终需要插入的是: hello 3
 *
 */
public class MysqlSinkTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // word count 单词统计数据, 通过 RichMapFunction 算子来写入redis
        // nc -lk 8888
        DataStream<Tuple2<String, Integer>> wordCountStream = WordCountStream.getWordCountStream(env);

        /*
        CREATE TABLE wc (
            word VARCHAR (16) NOT NULL PRIMARY KEY,
            word_count INT NOT NULL
        ) COMMENT = 'word count';
         */
        wordCountStream.addSink(new MysqlRichSinkFunction());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
