package com.gin.flink.sink.hbase.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


public class HBaseReaderSource extends RichSourceFunction<Tuple2<String, List<Cell>>> {


    //配置
    org.apache.hadoop.conf.Configuration config = null;
    //表管理
    Admin admin = null;
    private Connection conn = null;
    TableName tableName = null;
    private Table table = null;
    private Scan scan = null;


    @Override
    public void open(Configuration parameters) throws Exception {

        System.out.println("Read source open");
        super.open(parameters);
        //创建配置文件对象
        config = HBaseConfiguration.create();
        //加载ZK配置
        config.set("hbase.zookeeper.quorum", "node02,node03,node04");
        conn = ConnectionFactory.createConnection(config);
        //表管理对象
        admin = conn.getAdmin();
        //获取数据操作对象
        tableName = TableName.valueOf("psn");
        table = conn.getTable(tableName);
        scan = new Scan();
    }

    @Override
    public void run(SourceContext<Tuple2<String, List<Cell>>> sourceContext) throws Exception {
        System.out.println("Read source run");
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            List<Cell> cells = result.listCells();
            Tuple2<String, List<Cell>> tuple2 = new Tuple2<>();
            tuple2.setFields(rowkey, cells);
            sourceContext.collect(tuple2);
        }
    }

    @Override
    public void cancel() {
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