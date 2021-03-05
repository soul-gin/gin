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

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息生产,含重复消息
 */
public class TradeRocketMQProducer {

    private static final Logger log = LoggerFactory.getLogger(TradeRocketMQProducer.class);

    private static final int MESSAGE_NUM = 3;

    // Producer config
    private static final String NAME_SERVER_ADDR = "10.0.0.21:9876";
    private static final String PRODUCER_GROUP = "GID_SIMPLE_PRODUCER";
    private static final String TOPIC = "SOURCE_TOPIC";
    private static final String TAGS = "*";
    private static final String KEY_PREFIX = "KEY";

    private static RPCHook getAclRPCHook() {
        final String ACCESS_KEY = "${AccessKey}";
        final String SECRET_KEY = "${SecretKey}";
        return new AclClientRPCHook(new SessionCredentials(ACCESS_KEY, SECRET_KEY));
    }

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer(
                PRODUCER_GROUP, getAclRPCHook(), true, null);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);

        // When using aliyun products, you need to set up channels
        //producer.setAccessChannel(AccessChannel.CLOUD);
        producer.setAccessChannel(AccessChannel.LOCAL);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < MESSAGE_NUM; i++) {
                String content = "{\n" +
                        "  \"orderId\": \"571717\",\n" +
                        "  \"storeId\": 998877,\n" +
                        "  \"totalQty\": 1.0,\n" +
                        "  \"userId\": 56803793,\n" +
                        "  \"orderTotal\": {\n" +
                        "    \"amount\": 8.99,\n" +
                        "    \"cent\": 899,\n" +
                        "    \"currency\": \"CNY\"\n" +
                        "  }\n" +
                        "}";
                Message msg = new Message(TOPIC, TAGS, KEY_PREFIX + i, content.getBytes());
                try {
                    SendResult sendResult = producer.send(msg);
                    assert sendResult != null;
                    System.out.printf("send result: %s %s %s\n",
                            KEY_PREFIX + i, sendResult.getMsgId(), sendResult.getMessageQueue().toString());
                    Thread.sleep(10);
                } catch (Exception e) {
                    log.info("send message failed. {}", e.toString());
                }
            }
        }

    }
}
