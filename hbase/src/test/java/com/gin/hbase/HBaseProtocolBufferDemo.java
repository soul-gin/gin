package com.gin.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Random;


public class HBaseProtocolBufferDemo {

    //配置
    Configuration config = null;
    //连接
    Connection connection = null;
    //表管理
    Admin admin = null;
    //表名设置
    TableName tableName = TableName.valueOf("user");
    //表中列族
    private static String columnFamily = "cf";
    //数据操作
    Table table = null;

    @Before
    public void init() throws IOException {
        //创建配置文件对象
        config = HBaseConfiguration.create();
        //加载ZK配置
        config.set("hbase.zookeeper.quorum", "node02,node03,node04");
        connection = ConnectionFactory.createConnection(config);
        //表管理对象
        admin = connection.getAdmin();
        //获取数据操作对象
        table = connection.getTable(tableName);
    }

    @Test
    public void createTable() throws IOException {
        //定义表描述对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        //定义列族描述对象
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes());
        //添加列族信息给表
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        //创建表
        admin.createTable(tableDescriptorBuilder.build());
    }

    @Test
    public void insertBufBatch() throws IOException, ParseException {

        // 手动清理: truncate 'user'
        // 插入完成查看记录数: count 'user'
        //清空表
        admin.disableTable(tableName);
        admin.truncateTable(tableName, true);

        /*
        10个用户, 每个用户一年产生10000条通话记录
        rowkey设计: 手机号(测试,暂不翻转)+(Long.max - 时间戳)
         */
        LinkedList<Put> puts = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            //手机号
            String phoneNumber = getNumber("181");
            for (int j = 0; j < 10000; j++) {
                //对方手机号
                String opponentPhone = getNumber("177");
                //通话时长
                String connectTime = String.valueOf(random.nextInt(100));
                //通话时间
                String date = getDate("2021");
                //主动呼叫: 0  被呼叫: 1
                String type = String.valueOf(random.nextInt(2));

                //rowkey
                String rowkey = phoneNumber + "-" + (Long.MAX_VALUE - sdf.parse(date).getTime());

                //使用ProtocolBuffer
                //可以作为对象存储, 压缩存储空间, 但是就不能进行filter查询了
                //针对冷数据处理(不序列化存储25.24M, 序列化存储9.37M)
                Phone.MyPhoneDetail.Builder builder = Phone.MyPhoneDetail.newBuilder();
                builder.setOpponentPhone(opponentPhone);
                builder.setConnectTime(connectTime);
                builder.setDate(date);
                builder.setType(type);

                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("phone"), builder.build().toByteArray());
                puts.add(put);
            }
        }
        //批量插入
        table.put(puts);

    }

    private Random random = new Random();
    private String getNumber(String str) {
        // format 格式化输出
        // %d 表示数字
        // %s 表示字符串
        // 08 表示8位(0表示随机到0时, 用0占位)
        return str + String.format("%08d", random.nextInt(999999999));
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private String getDate(String str) {
        // format 格式化输出
        // %d 表示数字
        // %s 表示字符串
        // 02 表示2位(0表示随机到0时, 用0占位)
        //月+日+时+分+秒(测试考虑2月)
        return str + String.format("%02d%02d%02d%02d%02d", random.nextInt(11)+1, random.nextInt(27)+1,
                random.nextInt(24), random.nextInt(60), random.nextInt(60));
    }


    @Test
    public void getBuf() throws IOException {
        Get get = new Get(Bytes.toBytes("181960492523-9223370416181621807"));
        //获取指定列,减少IO
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("phone"));
        Result result = table.get(get);
        byte[] phones = CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("phone")));
        Phone.MyPhoneDetail myPhoneDetail = Phone.MyPhoneDetail.parseFrom(phones);

        System.out.println("myPhoneDetail=" + myPhoneDetail);
    }


    @After
    public void destroy() {
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
