-- 查看数据库
show databases;

-- 查看所在数据库
select currentDatabase();

-- 查看表
show tables;

-- Float32类型，从第8位开始产生溢出，会四舍五入
select toFloat32(0.123456789);

-- float存在精度损失
select 1-0.9;

-- Decimal避免精度损失
select
    toDecimal64(2.3656,3) as x,
    toTypeName(x) as xtype,
    toDecimal32(2.3656,2) as y,
    toTypeName(y) as ytype,
    x+y as z,
    toTypeName(z) as ztype;

-- FixedString, 定长, 补空
select toFixedString('hello',6) as a,length(a) as alength;

create table t_fixedString(
  id UInt8,
  name String,
  tp FixedString(2)
)engine = Memory();

insert into t_fixedString (id,name ,tp) values (1,'张三','a');
select * from t_fixedString;
-- \0 表示空
select * from t_fixedString where tp='a\0';


-- UUID
select generateUUIDv4();
create table t_uuid (
    id UInt8,
    name String,
    dept_id UUID
) engine = Memory();

insert into t_uuid values (1,'zs',generateUUIDv4()),(2,'ls',generateUUIDv4()), (2,'ls',generateUUIDv4());

select * from t_uuid;

-- Date  YYYY-MM-DD
select now(), toDate(now()), toDate('2020-12-09 12:23:43');
create table t_date(id UInt8,name String,birthday Date) engine = Memory();

-- DateTime  YYYY-MM-DD HH:MM:SS
select now(), toDateTime(now()), toDateTime('2020-09-13 23:43:12', 'Asia/Shanghai') as t, toTypeName(t);



-- DateTime64
select now(), toDateTime64(now(), 3, 'Asia/Shanghai'), toDateTime64('2020-12-09 12:23:43', 3);
CREATE TABLE t_dt(tm DateTime64(3, 'Europe/Moscow'),id UInt8) ENGINE = Memory();

-- Enum
Create table t_person(
    id UInt8,
    name String,
    gender Enum('男'=1,'女'=0)
)engine = Memory();

insert into t_person values (1,'zs','男'),(2,'ls','女');
insert into t_person values (3,'ww',1),(3,'ml',0);
select * from t_person;


-- 布尔类型, 没有布尔类型, 可以使用枚举代替
create table t_boolean(
 id UInt8,
 bl Enum8('true'=1,'false'=0)
)engine = Memory();
insert into t_boolean values (1,'true'),(2,'false');
select * from t_boolean;

-- Nullable 可以为空
create table t_nullable(
id UInt8,
name Nullable(String),
age Nullable(UInt8)
)engine = Memory();

insert into t_nullable values (1,'zs',18)(2,'ww',null)(3,null,20)(4,null,null);
select * from t_nullable;


-- 数组类型
create table t_array(
 id UInt8,
 name String,
 hobby Array(String)
)engine = Memory();

insert into t_array values (1,'zs',['xx1','xx2']),(2,'ls',array('xx3','xx4'));
select * from t_array;
select id,name,hobby[1] from t_array;

-- Tuple 元组类型有1~n个元素组成，每个元素允许设置不同的数据类型
create table t_tuple(
  id UInt8,
  name String,
  tp Tuple(String, UInt8)
) engine = Memory();

insert into t_tuple values (1,'zs',tuple('xx1',100)),(2,'ls',tuple('xx2',200));
select * from t_tuple;

-- Nested 使用场景较少; 嵌套类型, 一个或者多个嵌套数据类型字段, MergeTree引擎中不支持
create table t_nested(
    id UInt8,
    name String,
    dept_info Nested(
     dept_id UInt8,
     dept_name String
     )
) engine = Memory();
-- dept_id 和 dept_name个数需要保持一致
insert into t_nested values (1,'zs',array(111,222),array('detp1','dept2'));
select * from t_nested;

-- Domain 使用场景较少; 支持IPv4,IPv6, Pv4类型基于UInt32封装，IPv6基于FixedString(16)封装
-- 可以用字符串替代
create table t_domain(
 url String,
 ip  IPv4
)engine = Memory();
INSERT INTO t_domain(url, ip) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.tech', '183.247.232.58')('https://clickhouse.tech/docs/en/', '116.106.34.242');
select * from t_domain;






