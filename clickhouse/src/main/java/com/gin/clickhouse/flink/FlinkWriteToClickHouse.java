package com.gin.clickhouse.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author gin
 * @date 2021/3/29
 */
public class FlinkWriteToClickHouse {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // yum install nc -y
        // yum install nmap -y
        // netstat -natp |grep 8888
        // nc -lk 8888
        // 6,zl,26
        // 7,wb,26
        DataStreamSource<String> dss = env.socketTextStream("node01", 8888);
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> result = dss
                .map((MapFunction<String, Tuple3<Integer, String, Integer>>) value -> {
                    String[] split = value.split(",");
                    return new Tuple3<>(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer, String, Integer>>() {
                }));
        //String insetIntoCkSql = "insert into test (id,name,age) values (?,?,?)";
        String insetIntoCkSql = "insert into test values (?,?,?)";

        SinkFunction<Tuple3<Integer, String, Integer>> sink = JdbcSink.sink(
                // sql 语句
                insetIntoCkSql,
                // 配置从 DataStream 中如何获取数据, 来填补sql语句中的 "?"
                new JdbcStatementBuilder<Tuple3<Integer, String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement statement, Tuple3<Integer, String, Integer> value) throws SQLException {
                        statement.setInt(1, value.f0);
                        statement.setString(2, value.f1);
                        statement.setInt(3, value.f2);
                    }
                },
                //执行sql语句配置(没达到设置的批数执行一次)
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node01:8123/default")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("")
                        .build()

        );

        result.print();
        result.addSink(sink);

        try {
            env.execute("flink clickhouse sink");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
