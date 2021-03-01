package com.gin.flink.sink.hbase.utils.conn;

import org.apache.hadoop.conf.Configuration;

/**
 * @author gin
 * @date 2021/3/1
 */
public class HbaseConfig {

    protected Configuration configuration = null;
    private int poolSize;
    private int waitTimeMillis;

    public HbaseConfig(int poolSize, int waitTimeMillis,
                       Configuration configuration) {
        this.poolSize = poolSize;
        this.waitTimeMillis = waitTimeMillis;
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getWaitTimeMillis() {
        return waitTimeMillis;
    }

}