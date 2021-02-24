package com.gin.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Random;


public class HBaseDemo {

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
    public void deleteTable() throws IOException {
        if (admin.tableExists(tableName)) {
            //删除表需要先禁用
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    @Test
    public void insert() throws IOException {
        Put put = new Put(Bytes.toBytes("111"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes("gin"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"), Bytes.toBytes("26"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        table.put(put);
    }

    @Test
    public void insertBatch() throws IOException, ParseException {

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

                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("opponentPhone"), Bytes.toBytes(opponentPhone));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("connectTime"), Bytes.toBytes(connectTime));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("date"), Bytes.toBytes(date));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(type));
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
    public void getTest() throws IOException {
        //测试用, 列数量过多会导致IO瓶颈
        //get必须加限制, 否则会将所有的列都获取至本地
        Get get = new Get(Bytes.toBytes("111"));
        Result result = table.get(get);
        Cell cellName = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("name"));
        Cell cellAge = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("age"));
        Cell cellSex = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"));
        String name = Bytes.toString(CellUtil.cloneValue(cellName));
        String age = Bytes.toString(CellUtil.cloneValue(cellAge));
        String sex = Bytes.toString(CellUtil.cloneValue(cellSex));
        System.out.println("name=" + name + " age=" + age + " sex=" + sex);
    }

    @Test
    public void get() throws IOException {
        Get get = new Get(Bytes.toBytes("111"));
        //获取指定列,减少IO
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"));
        Result result = table.get(get);
        Cell cellName = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("name"));
        Cell cellAge = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("age"));
        Cell cellSex = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"));
        String name = Bytes.toString(CellUtil.cloneValue(cellName));
        String age = Bytes.toString(CellUtil.cloneValue(cellAge));
        String sex = Bytes.toString(CellUtil.cloneValue(cellSex));
        System.out.println("name=" + name + " age=" + age + " sex=" + sex);
    }

    @Test
    public void scanTest() throws IOException {
        //获取表中所有记录
        //实际需要注意scan的范围,避免冷数据价值到读缓存中
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell cellName = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("name"));
            Cell cellAge = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("age"));
            Cell cellSex = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("sex"));
            String name = Bytes.toString(CellUtil.cloneValue(cellName));
            String age = Bytes.toString(CellUtil.cloneValue(cellAge));
            String sex = Bytes.toString(CellUtil.cloneValue(cellSex));
            System.out.println("name=" + name + " age=" + age + " sex=" + sex);
        }

    }

    @Test
    public void scan() throws IOException, ParseException {
        //设置scan的范围,避免冷数据价值到读缓存中
        //注意rowkey设计, 前缀应该符合业务查询需求, 如: 业务标识+时间
        //根据业务标识散列情况, 正序或者倒序(防止数据倾斜, 前缀相同的在同一个region server)
        //如果是时间戳, 为了让最新数据在前面, 可以(Long.max - 时间戳)
        Scan scan = new Scan();
        //可以通过 count 'user' 来查看手机号数据
        //合理设置缓存, 指定每次拉取的数据(默认100)
        //一次请求1000条数据
        scan.setCaching(1000);
        //开始值
        String startRow = "181986635513-" + (Long.MAX_VALUE -sdf.parse("20210331000000").getTime());
        //结束值
        String endRow = "181986635513-" + (Long.MAX_VALUE -sdf.parse("20210301000000").getTime());
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            //System.out.println("-----");
            Cell opponentPhone = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("opponentPhone"));
            Cell connectTime = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("connectTime"));
            Cell date = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("date"));
            Cell type = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("type"));
            String opponentPhoneStr = Bytes.toString(CellUtil.cloneValue(opponentPhone));
            String connectTimeStr = Bytes.toString(CellUtil.cloneValue(connectTime));
            String dateStr = Bytes.toString(CellUtil.cloneValue(date));
            String typeStr = Bytes.toString(CellUtil.cloneValue(type));
            System.out.println("opponentPhone=" + opponentPhoneStr + " connectTime=" + connectTimeStr
                    + " date=" + dateStr + " type=" + typeStr);
        }
        //释放资源
        resultScanner.close();

    }

    @Test
    public void scanFilter() throws IOException, ParseException {
        //设置scan的范围,避免冷数据价值到读缓存中
        //注意rowkey设计, 前缀应该符合业务查询需求, 如: 业务标识+时间
        //根据业务标识散列情况, 正序或者倒序(防止数据倾斜, 前缀相同的在同一个region server)
        //如果是时间戳, 为了让最新数据在前面, 可以(Long.max - 时间戳)
        Scan scan = new Scan();
        //可以通过 count 'user' 来查看手机号数据
        //合理设置缓存, 指定每次拉取的数据(默认100)
        //一次请求1000条数据
        scan.setCaching(1000);
        //MUST_PASS_ALL 满足所有条件(and); MUST_PASS_ONE 满足一个条件(or)
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //type=1
        SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), CompareOperator.EQUAL, Bytes.toBytes("1"));
        filterList.addFilter(typeFilter);

        //前缀过滤( 针对 rowkey )
        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("181986635513-"));
        filterList.addFilter(prefixFilter);

        //添加过滤器列表
        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            //System.out.println("-----");
            Cell opponentPhone = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("opponentPhone"));
            Cell connectTime = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("connectTime"));
            Cell date = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("date"));
            Cell type = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("type"));
            String opponentPhoneStr = Bytes.toString(CellUtil.cloneValue(opponentPhone));
            String connectTimeStr = Bytes.toString(CellUtil.cloneValue(connectTime));
            String dateStr = Bytes.toString(CellUtil.cloneValue(date));
            String typeStr = Bytes.toString(CellUtil.cloneValue(type));
            System.out.println("opponentPhone=" + opponentPhoneStr + " connectTime=" + connectTimeStr
                    + " date=" + dateStr + " type=" + typeStr);
        }

        //释放资源
        resultScanner.close();

    }

    @Test
    public void delete() throws IOException, ParseException {
        // 查询 get 'user', '181986635513-9223370401903177807'
        Delete delete = new Delete(Bytes.toBytes("181986635513-9223370401903177807"));
        table.delete(delete);

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
