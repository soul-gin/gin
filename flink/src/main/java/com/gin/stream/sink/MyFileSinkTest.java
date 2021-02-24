package com.gin.stream.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

/**
 * @author gin
 * @date 2021/2/23
 */
public class MyFileSinkTest {

    public static void main(String[] args) {

        //<IN, BucketID>    IN:写入的数据的类型  BucketID：桶的名称的类型  日期+小时
        DefaultRollingPolicy<String, String> rolling = DefaultRollingPolicy.create()
                //文件5s钟没有写入新的数据，那么会产生一个新的小文件（滚动）
                .withInactivityInterval(5000)
                //当文件大小超过256M 也会滚动产生一个小文件
                .withMaxPartSize(256 * 1024 * 1024)
                //文件打开时间 超过10s 也会滚动产生一个小文件
                .withRolloverInterval(10000).build();

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("D:/var/flink/data"), new SimpleStringEncoder<String>("UTF-8"))
                //每隔一段时间 检测以下桶中的数据  是否需要回滚
                .withBucketCheckInterval(100)
                .withRollingPolicy(rolling)
                .build();


        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //连接socket获取输入的数据
        //将 socket 中的数据写入到hdfs(需要hadoop2.7以上)或者本地文件系统
        DataStreamSource<String> text = env.socketTextStream("node01", 8888, "\n");

        text.addSink(sink);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
