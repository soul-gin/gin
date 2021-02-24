package com.gin.flink.sink.hbase.batch;

import com.gin.flink.sink.hbase.batch.common.AbstractCustomTableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HBaseInputFormatAbstract extends AbstractCustomTableInputFormat<Tuple2<String, String>> {

    public String name = "psn";
    //配置
    org.apache.hadoop.conf.Configuration config = null;
    private Connection conn = null;
    TableName tableName = null;

    @Override
    public void configure(Configuration parameters) {
        try {
            super.configure(parameters);
            System.out.println("read source open");
            //创建配置文件对象
            config = HBaseConfiguration.create();
            //加载ZK配置
            config.set("hbase.zookeeper.quorum", "node02,node03,node04");
            conn = ConnectionFactory.createConnection(config);
            //获取数据操作对象
            tableName = TableName.valueOf(name);
            table = (HTable)conn.getTable(tableName);
            scan = new Scan();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Scan getScanner() {

        return scan;
    }

    @Override
    protected String getTableName() {
        return name;
    }

    @Override
    protected Tuple2<String, String> mapResultToTuple(Result result) {
        System.out.println("Read source run");
        String rowKey = Bytes.toString(result.getRow());
        List<Cell> cellList = result.listCells();
        System.out.println(cellList.size());
        StringBuilder sb = new StringBuilder();
        for (Cell cell : cellList) {
            String tmp = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            sb.append(tmp).append("_");
        }
        String value = sb.replace(sb.length() - 1, sb.length(), "").toString();
        Tuple2<String, String> tuple2 = new Tuple2<>();
        tuple2.setField(rowKey, 0);
        tuple2.setField(value, 1);
        return tuple2;
    }
}
