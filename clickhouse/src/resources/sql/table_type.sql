--
create database mydb;
use mydb;

-- 创建表t_tinylog 表，使用TinyLog引擎, 列式存储
create table t_tinylog(id UInt8,name String,age UInt8) engine=TinyLog;
insert into t_tinylog values (1,'张三',18),(2,'李四',19),(3,'王五',20);

-- 查询表中的数据
select * from t_tinylog;

-- 查看 /home/clickhouse/datadir/data/mydb/t_tinylog
-- 表t_tinylog中的每个列都单独对应一个*.bin文件
-- 同时还有一个sizes.json文件存储元数据, 记录了每个bin文件中数据大小
-- -rw-r----- 1 clickhouse clickhouse 29 Mar 26 21:06 age.bin
-- -rw-r----- 1 clickhouse clickhouse 29 Mar 26 21:06 id.bin
-- -rw-r----- 1 clickhouse clickhouse 48 Mar 26 21:06 name.bin
-- -rw-r----- 1 clickhouse clickhouse 90 Mar 26 21:06 sizes.json
