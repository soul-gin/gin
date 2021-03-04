/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gin.flink.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gin.flink.common.unrepeated.UnrepeatedKeyProcess;
import com.gin.flink.sink.hbase.stream.HBaseWriterSinkSingle;
import com.gin.flink.sink.hbase.utils.HBaseDAOImpl;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.SimpleTupleDeserializationSchema;

import java.io.IOException;
import java.util.Properties;

import static org.apache.rocketmq.flink.RocketMQConfig.CONSUMER_OFFSET_LATEST;
import static org.apache.rocketmq.flink.RocketMQConfig.DEFAULT_CONSUMER_TAG;

public class RocketMQToHBaseDemo {

    /**
     * 普通value state去重
     */
    public static void main(String[] args) throws Exception {
        //计算基本环境配置
        StreamExecutionEnvironment env = initEnv(args);
        //输入
        Properties consumerProps = getConsumerProps();
        SimpleTupleDeserializationSchema schema = new SimpleTupleDeserializationSchema();
        DataStreamSource<Tuple2<String, String>> source = env.addSource(
                new RocketMQSource<>(schema, consumerProps)).setParallelism(1);
        //source.print();

        //去重
        SingleOutputStreamOperator<Tuple2<String, String>> process = source.keyBy(0)
                .process(new UnrepeatedKeyProcess(), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        process.print();
        //计算
        SingleOutputStreamOperator<TradeCreateVO> flatMap = process.flatMap(new FlatMapFunction<Tuple2<String, String>, TradeCreateVO>() {

            @Override
            public void flatMap(Tuple2<String, String> tuple2, Collector<TradeCreateVO> collector) throws Exception {
                //MQ数据解析
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(tuple2.f1);
                /*LinkedList<String> skuList = new LinkedList<>();
                LinkedList<String> vendorList = new LinkedList<>();
                for (JsonNode jsonNode : root.get("itemList")) {
                    skuList.add(jsonNode.get("sku").asText());
                    vendorList.add(jsonNode.get("vendorId").asText());
                }
                String storeId = root.get("storeId").asText();
                */
                String orderId = root.get("orderId").asText();
                String userId = root.get("userId").asText();
                int totalQty = root.get("totalQty").asInt();
                long totalCent = root.get("orderTotal").get("cent").asLong();

                //累计
                HBaseDAOImpl hBaseDAO = new HBaseDAOImpl();
                //查询历史值
                Result result = hBaseDAO.getOneRow("psn2", userId);
                if (!result.isEmpty() ) {
                    Cell totalQtyCell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("totalQty"));
                    Cell totalCentCell = result.getColumnLatestCell(Bytes.toBytes("cf"), Bytes.toBytes("totalCent"));
                    totalQty += Integer.parseInt(Bytes.toString(CellUtil.cloneValue(totalQtyCell)));
                    totalCent += Long.parseLong(Bytes.toString(CellUtil.cloneValue(totalCentCell)));
                }
                TradeCreateVO tradeCreateVO = TradeCreateVO.builder().userId(userId).totalQty(totalQty).totalCent(totalCent).build();
                collector.collect(tradeCreateVO);

            }
        }).setParallelism(1);
        flatMap.print();

        //输出
        System.out.println("HBase Reader add sink");
        flatMap.addSink(new HBaseWriterSinkSingle()).setParallelism(1);

        //执行任务
        env.execute("rocketmq-to-hbase");
    }

    /**
     * Source Config
     *
     * @return properties
     */
    private static Properties getConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
                "10.0.0.21:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "GroupTest");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "SOURCE_TOPIC");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TAG, DEFAULT_CONSUMER_TAG);
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        consumerProps.setProperty(RocketMQConfig.ACCESS_CHANNEL, AccessChannel.CLOUD.name());
        return consumerProps;
    }

    private static StreamExecutionEnvironment initEnv(String[] args) throws IOException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // for local
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // for cluster
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // for remote
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("node01", 8081);

        env.getConfig().setGlobalJobParameters(params);

        // 每隔 180000ms 往数据源中插入一个barrier(批次标志位)
        env.enableCheckpointing(180000);
        // 设置checkpoint模式, 默认就是 CheckpointingMode.EXACTLY_ONCE (精确一次消费)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        /*
         * 设置checkpoint任务之间的间隔时间  checkpoint job1  checkpoint job2
         * 防止触发太密集的flink checkpoint，导致消耗过多的flink集群资源
         * 导致影响整体性能
         * 1000ms
         * 注意: 设置了这个参数, 表示checkpoint就应该是串行执行的;
         * 即: setMaxConcurrentCheckpoints(1), 否则当前配置会失效
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //设置checkpoint最大并行的个数
        // 1 表示串行执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //flink 任务取消之后，checkpoint数据是否删除
        // RETAIN_ON_CANCELLATION 当任务取消，checkpoints数据会保留
        // DELETE_ON_CANCELLATION 当任务取消，checkpoints数据会删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /*
        //rocksDBStateBackend 配置
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkDir/", true);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
        rocksDBStateBackend.setNumberOfTransferingThreads(2);
        rocksDBStateBackend.enableTtlCompactionFilter();
        env.setStateBackend(rocksDBStateBackend);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5 * 60 * 1000);*/

        return env;
    }


}
