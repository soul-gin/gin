-- 创建数据库
create database mydb;
use mydb;

-- Log系列表引擎
-- 数据被顺序append写到本地磁盘上。
-- 不支持delete、update修改数据。
-- 不支持index（索引）。
-- 不支持原子性写。如果某些操作(异常的服务器关闭)中断了写操作，则可能会获得带有损坏数据的表。
-- insert会阻塞select操作。当向表中写入数据时，针对这张表的查询会被阻塞，直至写入动作结束。

-- TinyLog(每列切分小文件)
-- 数据存储不分块，所以不支持并发数据读取，该引擎适合一次写入，多次读取的场景，
-- 对于处理小批量中间表的数据可以使用该引擎，这种引擎会有大量小文件，性能会低
-- 创建表 t_tinylog 表，使用TinyLog引擎, 列式存储
-- 不支持并发读取数据文件，查询性能较差；格式简单，适合用来暂存中间数据
create table t_tinylog(id UInt8,name String,age UInt8) engine=TinyLog;
insert into t_tinylog values (1,'张三',18);
insert into t_tinylog values (2,'李四',19);
insert into t_tinylog values (3,'王五',20), (4,'luffy',21);
-- Log系列 不支持删除/更新
-- alter table t_tinylog delete where id = 1;


-- 查询表中的数据
select * from t_tinylog;

-- 查看 /home/clickhouse/datadir/data/mydb/t_tinylog
-- 表t_tinylog中的每个列都单独对应一个*.bin文件
-- 同时还有一个sizes.json文件存储元数据, 记录了每个bin文件中数据大小
-- -rw-r----- 1 clickhouse clickhouse 29 Mar 26 21:06 age.bin
-- -rw-r----- 1 clickhouse clickhouse 29 Mar 26 21:06 id.bin
-- -rw-r----- 1 clickhouse clickhouse 48 Mar 26 21:06 name.bin
-- -rw-r----- 1 clickhouse clickhouse 90 Mar 26 21:06 sizes.json


-- StripLog(insert并发数据块)
-- 数据存储会划分块，每次插入对应一个数据块，拥有更高的查询性能
-- 拥有.mrk标记文件，支持并发读取数据文件，查询性能比TinyLog好；
-- 将所有列存储在同一个大文件中，减少了文件个数。
-- 向表t_stripelog中插入数据，这里插入分多次插入，会将数据插入不同的数据块中
create table t_stripelog(id UInt8,name String,age UInt8) engine = StripeLog;
insert into t_stripelog values (1,'张三',18);
insert into t_stripelog values (2,'李四',19);
insert into t_stripelog values (3,'王五',20), (4,'luffy',21);
select * from t_stripelog;

-- data.bin:数据文件，所有列字段都写入data.bin文件中。
-- index.mrk:数据标记文件，保存了数据在data.bin文件中的位置信息，即每个插入数据列的offset信息，
-- 利用数据标记能够使用多个线程，并行读取data.bin压缩数据，提升查询性能。
-- sizes.json:元数据文件，记录了data.bin和index.mrk大小信息。
-- -rw-r----- 1 clickhouse clickhouse 449 Mar 27 09:19 data.bin
-- -rw-r----- 1 clickhouse clickhouse 226 Mar 27 09:19 index.mrk
-- -rw-r----- 1 clickhouse clickhouse  69 Mar 27 09:19 sizes.json


-- Log(每列切分小文件+insert并发数据块)
-- 支持并发读取数据文件，查询性能比TinyLog好；每个列会单独存储在一个独立文件中。
-- Log引擎表适用于临时数据，一次性写入、测试场景。Log引擎结合了TinyLog表引擎和StripeLog表引擎的长处，
-- 是Log系列引擎中性能最高的表引擎。
-- 对于每一次的INSERT操作，会生成数据块，经测试，数据块个数与当前节点的core数一致
create table t_log(id UInt8,name String,age UInt8) engine = Log();
insert into t_log values (1,'张三',18);
insert into t_log values (2,'李四',19);
insert into t_log values (3,'王五',20), (4,'luffy',21);
select * from t_log;

-- t_log中的每个列都对应一个*.bin文件。其他两个文件的解释如下：
-- __marks.mrk：数据标记，保存了每个列文件中的数据位置信息，利用数据标记能够使用多个线程，
-- 并行度取data.bin压缩数据，提升查询性能。
-- sizes.json:记录了*.bin 和__mark.mrk大小的信息。
-- -rw-r----- 1 clickhouse clickhouse  82 Mar 27 09:26 age.bin
-- -rw-r----- 1 clickhouse clickhouse  82 Mar 27 09:26 id.bin
-- -rw-r----- 1 clickhouse clickhouse  48 Mar 27 09:26 __marks.mrk
-- -rw-r----- 1 clickhouse clickhouse 105 Mar 27 09:26 name.bin
-- -rw-r----- 1 clickhouse clickhouse 121 Mar 27 09:26 sizes.json


-- Special系列表引擎
-- Memory
-- Memory表引擎直接将数据保存在内存中，ClickHouse中的Memory表引擎具有以下特点:
-- 引擎以未压缩的形式将数据存储在 RAM 中，数据完全以读取时获得的形式存储。
-- 并发数据访问是同步的，锁范围小，读写操作不会相互阻塞。
-- 不支持索引。
-- 查询是并行化的，在简单查询上达到最大速率（超过10 GB /秒），在相对较少的行（最多约100,000,000）上有高性能的查询。
-- 没有磁盘读取，不需要解压缩或反序列化数据，速度更快（在许多情况下，与 MergeTree 引擎的性能几乎一样高）。
-- 重新启动服务器时，表存在，但是表中数据全部清空。
-- Memory引擎多用于测试。

create table t_memory(id UInt8 ,name String, age UInt8) engine = Memory;
insert into t_memory values (1,'张三',18),(2,'李四',19),(3,'王五',20);
select * from t_memory;
-- service clickhouse-server restart
-- 重启后表存在, 数据会清空
select * from t_memory;

-- Merge
-- Merge 引擎 (不要跟 MergeTree 引擎混淆) 本身不存储数据，但可用于同时从任意多个其他的表中读取数据，
-- 这里需要多个表的结构相同，并且创建的Merge引擎表的结构也需要和这些表结构相同才能读取。
-- 读是自动并行的，不支持写入。读取时，那些被真正读取到数据的表如果设置了索引，索引也会被使用。
create table t_a1 (id UInt8 ,name String,age UInt8) engine = TinyLog;
insert into t_a1 values (1,'张三',18),(2,'李四',19);
create table t_a2 (id UInt8 ,name String,age UInt8) engine = TinyLog;
insert into t_a2 values (3,'王五',20),(4,'马六',21);
create table t_a3 (id UInt8 ,name String,age UInt8) engine = TinyLog;
insert into t_a3 values (5,'田七',22),(6,'赵八',23);

-- 两个参数: 第一个表示合并 mydb 数据库的表, ^t_a 表示合并以t_a开头的表(注意正则不要匹配到其他的merge表)
create table t_merge (id UInt8,name String,age UInt8) engine = Merge(mydb,'^t_a');
select * from t_merge;


-- Distributed是ClickHouse中分布式引擎，之前所有的操作虽然说是在ClickHouse集群中进行的，
-- 但是实际上是在node1节点中单独操作的，与node2、node3无关，使用分布式引擎声明的表才可以在其他节点访问与操作。
-- Distributed引擎和Merge引擎类似，本身不存放数据,功能是在不同的server上把多张相同结构的物理表合并为一张逻辑表。
-- clickhouse-client -m
-- node01:
use default;
create table s1(id UInt8,name String,age UInt8) engine = Log();
insert into s1 values (1,'zs',18);
-- node02:
use default;
create table s1(id UInt8,name String,age UInt8) engine = Log();
insert into s1 values (2,'ls',19);
-- node03:
use default;
create table s1(id UInt8,name String,age UInt8) engine = Log();
insert into s1 values (3,'ww',20);
-- node01:
-- 集群名(配置 /etc/metrika.xml 中): clickhouse_cluster_3shards_1replicas
use default;
-- 集群名: clickhouse_cluster_3shards_1replicas, 数据库名: default, 每个节点上的表名: s1
-- sharding_key(可选的，用于分片的key值): id
create table t_cluster(id UInt8,name String,age UInt8) engine = Distributed(clickhouse_cluster_3shards_1replicas,default,s1,id);
select * from t_cluster;
-- 根据hash值插入到各个分片
insert into t_cluster values (5,'田七', 24),(6,'赵八', 26);
select * from t_cluster;

-- 真正集群中均可看到的表:(逻辑表)
create table t_cluster_all on cluster clickhouse_cluster_3shards_1replicas (id UInt8,name String,age UInt8) engine = Distributed(clickhouse_cluster_3shards_1replicas,default,s1,id);
show tables;
select * from t_cluster_all;
insert into t_cluster values (7,'高十', 24),(8,'萧十一', 26);
select * from t_cluster_all;


-- MergeTree系列表引擎
-- 在所有的表引擎中，最为核心的当属MergeTree系列表引擎，这些表引擎拥有最为强大的性能和最广泛的使用场合。
-- 对于非MergeTree系列的其他引擎而言，主要用于特殊用途，场景相对有限。而MergeTree系列表引擎是官方主推的存储引擎，
-- 有主键索引、数据分区、数据副本、数据采样、删除和修改等功能，支持几乎所有ClickHouse核心功能。
-- MergeTree系列表引擎包含：MergeTree、ReplacingMergeTree、SummingMergeTree（汇总求和功能）、
-- AggregatingMergeTree（聚合功能）、CollapsingMergeTree（折叠删除功能）、
-- VersionedCollapsingMergeTree（版本折叠功能）引擎，在这些的基础上还可以叠加Replicated和Distributed

-- MergeTree作为家族系列最基础的表引擎，主要有以下特点：
-- 存储的数据按照主键排序：创建稀疏索引加快数据查询速度。
-- 支持数据分区，可以通过 PARTITION BY 语句指定分区字段。
-- 支持数据副本。
-- 支持数据采样。

-- primary key 不是用于主键去重, 而是用于稀疏索引(排序后抽取值, 索引范围数据), 类似跳跃表(skip list)
-- ENGINE：ENGINE = MergeTree()，MergeTree引擎没有参数。
-- ORDER BY：排序字段。比如ORDER BY (Col1, Col2)，值得注意的是，如果没有使用 PRIMARY KEY ,
-- 显式的指定主键ORDER BY排序字段自动作为主键。如果不需要排序，则可以使用 ORDER BY tuple() 语法，
-- 这样的话，创建的表也就不包含主键。这种情况下，ClickHouse会按照插入的顺序存储数据。必选项。
-- PARTITION BY：分区字段，例如要按月分区，可以使用表达式 toYYYYMM(date_column)，
-- 这里的date_column是一个Date类型的列，分区名的格式会是"YYYYMM"。可选。
-- PRIMARY KEY：指定主键，如果排序字段与主键不一致，可以单独指定主键字段。否则默认主键是排序字段。
-- 大部分情况下不需要再专门指定一个 PRIMARY KEY 子句，注意，在MergeTree中主键并不用于去重，
-- 而是用于索引，加快查询速度。可选。
-- 另外，如果指定了PRIMARY KEY与排序字段不一致，要保证PRIMARY KEY 指定的主键是ORDER BY 指定字段的前缀
-- SAMPLE BY：采样字段，如果指定了该字段，那么主键中也必须包含该字段。
-- 比如 SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))。可选。
-- TTL：数据的存活时间。在MergeTree中，可以为某个列字段或整张表设置TTL。当时间到达时，
-- 如果是列字段级别的TTL，则会删除这一列的数据；如果是表级别的TTL，则会删除整张表的数据。可选。
-- SETTINGS：额外的参数配置。可选。
create table t_mt
(
    id       UInt8,
    name     String,
    age      UInt8,
    birthday Date
)
    engine = MergeTree
        order by id
        partition by toYYYYMM(birthday)
        primary key id;

insert into t_mt values (1,'zs',18,'2020-01-01'),(1,'ls',19,'2020-02-01');
insert into t_mt values (3,'ww',20,'2020-01-11'),(4,'ml',21,'2020-02-01');
insert into t_mt values (5,'xx',20,'2020-01-11'),(6,'xx2',21,'2020-02-21');
select * from t_mt;
-- 测试合并: 新插入的数据会新生成数据块
optimize table t_mt;
-- optimize table t_mt final;
-- 合并指定分区
optimize table t_mt partition '202002';
-- 查看表属性
select table ,partition ,name ,active from system.parts where table = 't_mt';
-- 新增列
alter table t_mt add column loc String;
show create table t_mt;
-- 删除列
alter table t_mt drop column loc;
-- 注释列
alter table t_mt comment column name '姓名';
show create table t_mt;
-- 情空列数据
alter table t_mt clear column name in partition '202001';
select * from t_mt;
-- 改名
rename table t_mt to t_merge_tree;


-- table: 代表当前表。
-- partition: 是当前表的分区名称。
-- name: 是对应到磁盘上数据所在的分区目录片段。例如“202102_2_2_0”中“202102”是分区名称，
-- “2”是数据块的最小编号，“2”是数据块的最大编号，“0”代表该块在MergeTree中第几次合并得到。
-- active: 代表当前分区片段的状态：1代表激活状态，0代表非激活状态，
-- 非激活片段是那些在合并到较大片段之后剩余的源数据片段，损坏的数据片段也表示为非活动状态。
-- 非激活片段会在合并后的10分钟左右被删除。



-- /opt/middleware/clickhouse/datadir/data/mydb/t_mt

-- drwxr-x--- 2 clickhouse clickhouse 264 Mar 27 14:15 202001_1_1_0
-- drwxr-x--- 2 clickhouse clickhouse 264 Mar 27 14:15 202001_3_3_0
-- drwxr-x--- 2 clickhouse clickhouse 264 Mar 27 14:15 202001_5_5_0
-- drwxr-x--- 2 clickhouse clickhouse 264 Mar 27 14:15 202002_2_2_0
-- drwxr-x--- 2 clickhouse clickhouse 264 Mar 27 14:15 202002_4_4_0
-- drwxr-x--- 2 clickhouse clickhouse 264 Mar 27 14:15 202002_6_6_0
-- drwxr-x--- 2 clickhouse clickhouse   6 Mar 27 14:14 detached
-- -rw-r----- 1 clickhouse clickhouse   1 Mar 27 14:14 format_version.txt



-- checksums.txt：校验文件，使用二进制格式存储。
-- 它保存了余下各类文件(primary. idx、count.txt等)的size大小及size的哈希值，
-- 用于快速校验文件的完整性和正确性。
-- columns.txt： 存储当前分区所有列信息。使用明文格式存储。
-- count.txt：计数文件，使用明文格式存储。用于记录当前数据分区目录下数据的总行数。
-- primary.idx：一级索引文件，使用二进制格式存储。用于存放稀疏索引，
-- 一张MergeTree表只能声明一次一级索引，即通过ORDER BY或者PRIMARY KEY指定字段。
-- 借助稀疏索引，在数据查询的时能够排除主键条件范围之外的数据文件，从而有效减少数据扫描范围，加速查询速度。
-- 列.bin：数据文件，使用压缩格式存储，默认为LZ4压缩格式，用于存储某一列的数据。
-- 由于MergeTree采用列式存储，所以每一个列字段都拥有独立的.bin数据文件，并以列字段名称命名。
-- 列.mrk2：列字段标记文件，使用二进制格式存储。标记文件中保存了.bin文件中数据的偏移量信息
-- partition.dat与minmax_[Column].idx：如果指定了分区键，则会额外生成partition.dat与minmax索引文件，
-- 它们均使用二进制格式存储。partition.dat用于保存当前分区下分区表达式最终生成的值，即分区字段值；
-- 而minmax索引用于记录当前分区下分区字段对应原始数据的最小和最大值。
-- 比如当使用birthday字段对应的原始数据为2021-02-17、2021-02-23，
-- 分区表达式为PARTITION BY toYYYYMM(birthday)，即按月分区。partition.dat中保存的值将会是202102，
-- 而minmax索引中保存的值将会是2021-02-17、2021-02-23。

-- 查询: 找到对应分区文件夹 -> primary.idx找到索引 ->
-- 拿到查询数据的对应列的偏移量name.mrk2 -> 根据偏移量去数据源中获取name.bin

-- -rw-r----- 1 clickhouse clickhouse  27 Mar 27 14:15 age.bin
-- -rw-r----- 1 clickhouse clickhouse  48 Mar 27 14:15 age.mrk2
-- -rw-r----- 1 clickhouse clickhouse  28 Mar 27 14:15 birthday.bin
-- -rw-r----- 1 clickhouse clickhouse  48 Mar 27 14:15 birthday.mrk2
-- -rw-r----- 1 clickhouse clickhouse 440 Mar 27 14:15 checksums.txt
-- -rw-r----- 1 clickhouse clickhouse  90 Mar 27 14:15 columns.txt
-- -rw-r----- 1 clickhouse clickhouse   1 Mar 27 14:15 count.txt
-- -rw-r----- 1 clickhouse clickhouse  27 Mar 27 14:15 id.bin
-- -rw-r----- 1 clickhouse clickhouse  48 Mar 27 14:15 id.mrk2
-- -rw-r----- 1 clickhouse clickhouse   4 Mar 27 14:15 minmax_birthday.idx
-- -rw-r----- 1 clickhouse clickhouse  29 Mar 27 14:15 name.bin
-- -rw-r----- 1 clickhouse clickhouse  48 Mar 27 14:15 name.mrk2
-- -rw-r----- 1 clickhouse clickhouse   4 Mar 27 14:15 partition.dat
-- -rw-r----- 1 clickhouse clickhouse   2 Mar 27 14:15 primary.idx


-- String partition
create table t_partition (id UInt8,name String ,loc String) engine = MergeTree
order by id
partition by loc;
insert into t_partition values (1,'zs','北京'),(2,'ls','sh'),(3,'ww','北京'),(4,'ml','sh');
optimize table t_partition;
-- 注意String类型的 partition 文件名使用的是String值的 hashcode 处理后的值
-- sh : 639d654382e2ba7be8162fd75112ebd1_2_2_0
-- 北京: a6155dcc1997eda1a348cd98b17a93e9_1_1_0
select table ,partition ,name ,active from system.parts where table = 't_partition';

-- 卸载分区(数据导出场景, 只导出部分数据), 放在 detached 文件夹下
alter table t_partition detach partition 'sh';
select * from t_partition;

-- 加载分区
alter table t_partition attach partition 'sh';
select * from t_partition;

-- 删除分区
alter table t_partition drop partition 'sh';
select * from t_partition;

-- 替换分区(复制分区), 源分区数据不会被删除
create table t_partition2 (id UInt8,name String ,loc String) engine = MergeTree
                                                                order by id
                                                                partition by loc;
insert into t_partition2 values (1,'zs','北京'),(2,'ls','sh'),(3,'ww','北京'),(4,'ml','sh');
alter table t_partition replace partition 'sh' from t_partition2;
select * from t_partition;
-- clickhouse不建议使用 update delete
alter table t_partition update name = 'www' where name = 'ww';
select * from t_partition;
alter table t_partition delete where name = 'www';
select * from t_partition;

-- 临时表, 会话结束表则删除(可以使用内存表替代)
create temporary table t_partition2 (id UInt8,name String,loc String) ;


--- 创建普通视图 ---
create table t_v_mt(id UInt8,name String,age UInt8) engine = MergeTree()
order by id;

-- 普通视图不存储数据
insert into t_v_mt values (1,'zs',18),(2,'ls',19);
create view t_view as select id,name ,age from t_v_mt;
select * from t_view;

create view t_view2 as select id,count(age) as total_age from t_v_mt group by id;
select * from t_view2;

-- 源表删除, 普通视图无法使用
-- drop table t_v_mt;
-- select * from t_view;


-- 创建物化视图(将原有数据也按物化视图逻辑进行计算: populate)
create materialized view t_materialized engine = MergeTree() order by id  populate
as select id,sum(age) as total_age from t_v_mt group by id;
select * from t_materialized;
-- drop table t_v_mt 源表被删除, 物化视图依然有数据









