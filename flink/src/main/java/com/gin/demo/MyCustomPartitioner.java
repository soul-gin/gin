package com.gin.demo;


import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author gin
 * @date 2021/2/23
 *
 * 自定义下沉分区策略
 *
 */
public class MyCustomPartitioner implements Partitioner<Long> {

    @Override
    public int partition(Long key, int numPartitions) {
        return key.intValue() % numPartitions;
    }

}
