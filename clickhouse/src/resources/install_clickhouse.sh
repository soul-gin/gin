#!/bin/bash
rpm -ivh ./clickhouse-*.rpm
echo "ln -s /var/lib/clickhouse datadir"
echo "service clickhouse-server start"
echo "ln -s /etc/clickhouse-server conf"
echo "ln -s /var/log/clickhouse-server log"
echo "clickhouse-client"
echo "show databases"

# DBeaver 连接, 或者内嵌 tabix :
# 解开下面配置, 可以访问管理页面 http://ip:8123
#     <!--
#    <http_server_default_response><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></http_server_default_response>
#    -->