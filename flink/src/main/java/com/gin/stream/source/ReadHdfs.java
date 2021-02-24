package com.gin.stream.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.Path;

/**
 * @author gin
 * @date 2021/2/20
 */
public class ReadHdfs {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 查看hdfs中hdfs-site.xml的 dfs.namenode.rpc-address 配置的端口
        /*
        <!-- nn1的RPC通信地址,8020取代了之前默认配置的 9000 端口,区分HA和standalone
             netstat -ntp | grep 8020 -->
        <property>
          <name>dfs.namenode.rpc-address.mycluster.nn1</name>
          <value>node01:8020</value>
        </property>
         */

        try {
            String filePath = "hdfs://node01:8020/data/wc/input";
            TextInputFormat textInputFormat = new TextInputFormat(new Path(filePath));
            DataSource<String> stream = env.readFile(textInputFormat, filePath);
            stream.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
