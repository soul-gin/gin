package com.gin.flink.sink.hbase.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author gin
 * @date 2021/2/25
 */
@Slf4j
public class HbaseConnectUtils {

    /**
     * 连接是线程安全, 可共享单例, 重量级
     */
    private static Connection conn = null;

    static {
        try {
            conn = ConnectionFactory.createConnection(getHbaseConf());
        } catch (IOException e) {
            log.error("initTables table error:" + e);
        }
    }

    public static Connection getHbaseConn() {
        // 创建 HBase 连接，在程序生命周期内只需创建一次，该连接线程安全，可以共享给所有线程使用。
        // 在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。
        // 也可以采用try finally方式防止泄露
        return conn;
    }

    private static Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create();
        // 公用配置
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        // AbstractRpcClient HConstants.HBASE_CLIENT_IPC_POOL_TYPE
        //conf.set("hbase.client.ipc.pool.type","Reusable");
        //conf.set("hbase.client.ipc.pool.size","10");
        return conf;
    }

}
