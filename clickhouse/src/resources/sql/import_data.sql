---csv数据导入、导出----
create table t_csv(id UInt8,name String,age UInt8) engine = MergeTree  order by id;

-- 生成 csv 文件
-- cat >> a.csv <<EOF
-- 1,zs,18
-- 2,ls,28
-- 3,ww,38
-- EOF

-- 导入
-- clickhouse-client --format_csv_delimiter="," --query="INSERT INTO mydb.t_csv FORMAT CSV" < /opt/middleware/clickhouse/a.csv
select * from t_csv;
-- 导出, 将分隔符指定为 |
-- clickhouse-client --format_csv_delimiter="|" --query="select * from mydb.t_csv FORMAT CSV" > /opt/middleware/clickhouse/b.csv
-- cat b.csv


------------ HDFS ---------------
-- hdfs dfs -mkdir ch
-- hdfs dfs -put ./*.csv
-- 使用通配符 *.csv 是不能插入数据的
create table t_hdfs (id UInt8,name String,age UInt8) engine = HDFS('hdfs://node01:8020/ch/*.csv','CSV');
-- 使用hdfs的集群名称(mycluster)方式:
-- 1） 将hadoop路径下$HADOOP_HOME/etc/hadoop下的hdfs-site.xml文件复制到/etc/clickhouse-server目录下。
-- 2） 修改/etc/init.d/clickhouse-server 文件，加入一行 “export LIBHDFS3_CONF=/etc/clickhouse-server/hdfs-site.xml”
-- 3） 重启ClickHouse-server 服务
-- serveice clickhouse-server restart

-- 不使用通配符 *.csv 是能插入数据的
create table t_hdfs2 (id UInt8,name String,age UInt8) engine = HDFS('hdfs://mycluster/ttt.csv','CSV');
insert into t_hdfs2 values (10, 'zz', 19);


------ mysql ----
-- 和数据库MYSQL引擎类型, 不过这里是表引擎, 操作的是表
-- ip:port 数据库名, 表名, 用户名, 密码
create table t_mysql (id UInt8,name String,age UInt8) engine = MySQL('node01:3306','mysqldb','info','root','123456');
-- 1 表示遇到主键相同的数据则更新
drop table t_mysql;
create table t_mysql (id UInt8,name String,age UInt8) engine = MySQL('node01:3306','mysqldb','info','root','123456',1);
-- 0 表示遇到主键相同的数据只更新 name 字段, 其他字段不更新
drop table t_mysql;
create table t_mysql (id UInt8,name String,age UInt8) engine = MySQL('node01:3306','mysqldb','info','root','123456',0,'update name = values(name)');


------- Kafka ------
-- csv 格式消费
-- 注意先创建 topic 或者 直接使用 kafka-console-producer.sh
-- kafka-console-producer.sh --broker-list node01:9092  --topic t1
-- 1,zs1,19
-- 2,ls2,20
-- 3,ww2,22
create table t_kafka (id UInt8,name String,age UInt8) engine = Kafka()
settings
kafka_broker_list = 'node01:9092,node02:9092,node03:9092',
kafka_topic_list = 't1',
kafka_group_name = 'group_name_test',
kafka_format = 'CSV';

-- 为留存kafka消费后的数据(t_kafka 不会保留数据, 消费完成则丢弃)
-- 方式一: 创建物化视图引擎查询t_kafka中的数据
create materialized view t_kafka_view engine = MergeTree order by id as select * from t_kafka;
select * from t_kafka_view;
drop table t_kafka_view;

-- 方式二: 创建普通MergeTree表 + materialized表(一般 view 会和 源表 同时被删除)
-- 为了在 t_kafka 被删除后, 对应删除 materialized, 数据依然留存在 普通MergeTree 中
create table t_mymt (id UInt8,name String,age UInt8) engine = MergeTree() order by id;
-- 将数据从kafka中获取后, 存入物化view 和 普通MergeTree表
create materialized view t_kafka_view to t_mymt as select * from t_kafka;
select * from t_kafka_view;
select * from t_mymt;


-- json 格式消费
-- 注意先创建 topic 或者 直接使用 kafka-console-producer.sh
-- kafka-console-producer.sh --broker-list node01:9092  --topic t2
-- {"id":1,"name":"zs","age":19}
-- {"id":2,"name":"ls","age":20}
-- {"id":3,"name":"ww","age":22}
-- {"id":4,"name":"wssw","age":11}
create table t_kafka2 (id UInt8,name String,age UInt8) engine = Kafka()
settings
    kafka_broker_list = 'node01:9092,node02:9092,node03:9092',
    kafka_topic_list = 't2',
    kafka_group_name = 'xx1',
    kafka_format = 'JSONEachRow';

create materialized view t_kafka_view2 engine = MergeTree order by id as select * from t_kafka2;

select * from t_kafka_view2;






