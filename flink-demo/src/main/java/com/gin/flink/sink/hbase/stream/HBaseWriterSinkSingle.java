package com.gin.flink.sink.hbase.stream;

import com.gin.flink.demo.TradeCreateVO;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class HBaseWriterSinkSingle extends RichSinkFunction<TradeCreateVO> {

    //配置
    org.apache.hadoop.conf.Configuration config = null;
    //表管理
    Admin admin = null;
    private Connection conn = null;
    TableName tableName = null;
    private Table table = null;

    AtomicInteger count = new AtomicInteger(0);


    @Override
    public void open(Configuration parameters) throws Exception {
        count.incrementAndGet();
        System.out.println("Read source open" + count);
        super.open(parameters);
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
    public void invoke(TradeCreateVO value, Context context) throws Exception {
        Put put = new Put(Bytes.toBytes(value.getUserId()));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("totalQty"), Bytes.toBytes(String.valueOf(value.getTotalQty())));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("totalCent"), Bytes.toBytes(String.valueOf(value.getTotalCent())));
        table.put(put);
    }


    @Override
    public void close() {
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

}