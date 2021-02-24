package com.gin.stream.sink;

import com.gin.stream.WordCountStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/**
 * @author gin
 * @date 2021/2/23
 *
 * 将WordCount的计算结果写入到Hbase中
 * 注意：Flink是一个流式计算框架
 * 会出现需要插入: hello 1;  hello 3;
 * 最终需要插入的是: hello 3
 *
 * 通过 process 方式插入(数据处理过程中,也可以将数据写入存储层)
 * sink方式只能单纯写入,不能在sink过程数据处理
 *
 */
public class HbaseSinkTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // word count 单词统计数据, 通过 RichMapFunction 算子来写入redis
        // nc -lk 8888
        DataStream<Tuple2<String, Integer>> wordCountStream = WordCountStream.getWordCountStream(env);

        //sink方式只能单纯写入,不能在sink过程数据处理
        wordCountStream.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //配置
            org.apache.hadoop.conf.Configuration config = null;
            //表管理
            Admin admin = null;
            private Connection conn = null;
            TableName tableName = null;
            private Table table = null;

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                //key=单词名 value=单词出现次数
                Put put = new Put(Bytes.toBytes(value.f0));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("userId"), Bytes.toBytes(String.valueOf(value.f1)));
                //查询: hbase shell
                // get 'psn2','hello1'
                table.put(put);

                out.collect(value);
            }

            @Override
            public void open(Configuration parameters) throws Exception {

                //创建配置文件对象
                config = HBaseConfiguration.create();
                //加载ZK配置
                config.set("hbase.zookeeper.quorum", "node02,node03,node04");
                conn = ConnectionFactory.createConnection(config);
                //表管理对象
                admin = conn.getAdmin();
                //获取数据操作对象
                tableName = TableName.valueOf("psn2");
                table = conn.getTable(tableName);

            }

            @Override
            public void close() throws Exception {
                try {
                    if (table != null) {
                        table.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                } catch (IOException e) {
                    System.out.println("Close HBase Exception:" + e.toString());
                }
            }
        });

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
