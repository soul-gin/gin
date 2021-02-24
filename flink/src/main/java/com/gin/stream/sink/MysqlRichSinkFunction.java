package com.gin.stream.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.DriverManager;

/**
 * @author gin
 * @date 2021/2/23
 */
public class MysqlRichSinkFunction extends RichSinkFunction<Tuple2<String, Integer>> {

    private Connection conn;
    private PreparedStatement updatePst;
    private PreparedStatement insertPst;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "gin", "123456");
        updatePst = conn.prepareStatement("update wc set word_count = ? where word = ?");
        insertPst = conn.prepareStatement("insert into wc values(?,?)");
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        //更新数据
        updatePst.setInt(1, value.f1);
        updatePst.setString(2, value.f0);
        updatePst.execute();
        //如果没有更新,表示记录不存在
        if (updatePst.getUpdateCount() == 0) {
            //插入数据
            insertPst.setString(1, value.f0);
            insertPst.setInt(2, value.f1);
            insertPst.execute();
        }
    }


    @Override
    public void close() throws Exception {
        updatePst.close();
        insertPst.close();
        conn.close();
    }


}
