-- Ordinary 就是ClickHouse中默认数据库引擎
create database test1 engine = Ordinary;
-- Ordinary 为默认值, 可以不用指定
create database test2;
show create database test1;
show create database test2;

-- 删除
drop database test1;
drop database test2;

-- MySQL数据库引擎
-- MySQL引擎用于将远程的MySQL服务器中的表映射到ClickHouse中
-- 并允许对表进行INSERT插入和SELECT查询
-- 方便在ClickHouse与MySQL之间进行数据交换
-- 这里不会将MySQL的数据同步到ClickHouse中
-- ClickHouse就像一个壳子

-- 登录mysql 在mysql中创建test数据库
-- mysql> create database test;
-- 在mysql test库中新建表 mysql_table
-- mysql> use test;
-- mysql> create table mysql_table(id int ,name varchar(255));
-- 向mysql表 mysql_table中插入两条数据
-- mysql> insert into mysql_table values (1,"zs"),(2,"ls");
-- mysql> select * from mysql_table;

CREATE DATABASE mysql_db ENGINE = MySQL('node01:3306', 'test', 'root', '123456');
use mysql_db;
show tables;
desc mysql_table;
-- 是一个单独的引擎库, 需先切换到对应库进行操作
select * from mysql_table;
insert into mysql_table values (3,'ww');
select * from mysql_table;

-- 内存数据库引擎
-- 测试用
-- engine = Memory()