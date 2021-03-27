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
create table t_hdfs (id UInt8,name String,age UInt8) engine = HDFS('hdfs://mycluster/ch/*.csv','CSV');

create table t_hdfs2 (id UInt8,name String,age UInt8) engine = HDFS('hdfs://mycluster/ttt.csv','CSV');

------ mysql ----

create table t_mysql (id UInt8,name String,age UInt8) engine = MySQL('node2:3306','mysqldb','info','root','123456');

create table t_mysql (id UInt8,name String,age UInt8) engine = MySQL('node2:3306','mysqldb','info','root','123456',1);

create table t_mysql (id UInt8,name String,age UInt8) engine = MySQL('node2:3306','mysqldb','info','root','123456',0,'update name = values(name)');

------- Kafka ------
create table t_kafka (id UInt8,name String,age UInt8) engine = Kafka()
settings
kafka_broker_list = 'node1:9092,node2:9092,node3:9092',
kafka_topic_list = 't1',
kafka_group_name = 'group_name',
kafka_format = 'CSV';

-- 测试
-- 创建物化视图引擎查询t_kafka中的数据
create materialized view t_kafka_view engine = MergeTree order by id as select * from t_kafka;


-- 创建普通MergetTree表
create table t_mymt (id UInt8,name String,age UInt8) engine = MergeTree() order by id;


create materialized view t_kafka_view  to t_mymt as select * from t_kafka;


----
create table t_kafka (id UInt8,name String,age UInt8) engine = Kafka()
settings
    kafka_broker_list = 'node1:9092,node2:9092,node3:9092',
    kafka_topic_list = 't2',
    kafka_group_name = 'xx1',
    kafka_format = 'JSONEachRow';

-- {"id":1,"name":"zs","age":19}
-- {"id":2,"name":"ls","age":20}
-- {"id":3,"name":"ww","age":22}
-- {"id":4,"name":"wssw","age":11}







