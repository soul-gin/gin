#!/bin/bash
rpm -ivh ./clickhouse-*.rpm
echo "ln -s /var/lib/clickhouse datadir"
echo "service clickhouse-server start"
echo "ln -s /etc/clickhouse-server conf"
echo "ln -s /var/log/clickhouse-server log"
echo "clickhouse-client"
echo "show databases"