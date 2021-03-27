#!/bin/bash
rpm -ivh ./clickhouse-*.rpm
echo "ln -s /var/lib/clickhouse datadir"
echo "service clickhouse-server start"
echo "ln -s /etc/clickhouse-server conf"
echo "ln -s /var/log/clickhouse-server log"
echo "clickhouse-client"
echo "show databases"

sed -i 's|<!-- <listen_host>::</listen_host> -->|<listen_host>::</listen_host>|g' /etc/clickhouse-server/config.xml

# macros 区分每台clickhouse节点的宏配置, 每台值应不同, 01, 02, 03
#<macros>
#    <replica>01</replica>
#</macros>
cat >> /etc/metrika.xml <<EOF
<yandex>
    <clickhouse_remote_servers>
        <clickhouse_cluster_3shards_1replicas>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node01</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <internal_replication>true</internal_replication>
                    <host>node02</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>node03</host>
                    <port>9000</port>
                </replica>
            </shard>
        </clickhouse_cluster_3shards_1replicas>
    </clickhouse_remote_servers>

    <zookeeper-servers>
        <node index="1">
            <host>node02</host>
            <port>2181</port>
        </node>
        <node index="2">
            <host>node03</host>
            <port>2181</port>
        </node>
        <node index="3">
            <host>node04</host>
            <port>2181</port>
        </node>
    </zookeeper-servers>
    <macros>
        <replica>01</replica>
    </macros>
    <networks>
        <ip>::/0</ip>
    </networks>
    <clickhouse_compression>
        <case>
            <min_part_size>10000000000</min_part_size>
            <min_part_size_ratio>0.01</min_part_size_ratio>
            <method>lz4</method>
        </case>
    </clickhouse_compression>
</yandex>
EOF