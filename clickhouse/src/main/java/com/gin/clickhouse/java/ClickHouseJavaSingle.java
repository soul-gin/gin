package com.gin.clickhouse.java;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.ResultSet;

/**
 *
 * create table test(id UInt8 ,name String, age UInt8) engine = Memory;
 * insert into test values (1,'张三',18),(2,'李四',19),(3,'王五',20);
 *
 * @author gin
 * @date 2021/3/29
 */
public class ClickHouseJavaSingle {

    public static void main(String[] args) {
        try {
            ClickHouseProperties props = new ClickHouseProperties();
            props.setUser("default");
            props.setPassword("");
            //连接配置 node01
            BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://node01:8123/default", props);
            //获取连接
            ClickHouseConnection conn = dataSource.getConnection();
            //查询语句对象
            ClickHouseStatement statement = conn.createStatement();
            //查询数据
            ResultSet rs = statement.executeQuery("select id,name,age from test");
            while(rs.next()){
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int age = rs.getInt("age");
                System.out.println("id = "+id+",name = "+name +",age = "+age);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
