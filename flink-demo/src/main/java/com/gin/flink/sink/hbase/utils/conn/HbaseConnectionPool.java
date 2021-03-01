package com.gin.flink.sink.hbase.utils.conn;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 正常使用 HbaseConnectUtils 的单例即可, 线程安全, 特殊需求使用连接池
* @author gin
* @date 2021/3/1
*/
public class HbaseConnectionPool {
    private static final Logger logger = LogManager.getLogger(HbaseConnectionPool.class);

    private List<HbaseConnection> busyConnection;
    private List<HbaseConnection> idleConnection;
    /**
     * 集群配置
     */
    private HbaseConfig hbaseClusterConfig = null;
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    public int init(HbaseConfig hbaseConfig) {
        if (hbaseConfig == null) {
            return -1;
        }
        idleConnection = new LinkedList<HbaseConnection>();
        busyConnection = new LinkedList<HbaseConnection>();
        this.hbaseClusterConfig = hbaseConfig;

        int ret;
        HbaseConnection connection;
        for (int i = 0; i < hbaseConfig.getPoolSize(); ++i) {
            connection = new HbaseConnection();
            ret = connection.initConnection(hbaseConfig);
            if (0 != ret) {
                logger.warn("init connection failed.");
                return -1;
            }
            idleConnection.add(connection);
            logger.debug("add connection success");
        }
        return 0;
    }


    public void clearPool() {
        lock.lock();
        try {
            for (HbaseConnection connection : idleConnection) {
                if (connection != null) {
                    connection.getConnection().close();
                }
            }
            for (HbaseConnection connection : busyConnection) {
                if (connection != null) {
                    connection.getConnection().close();
                }
            }
            busyConnection.clear();
            idleConnection.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }


    public void resetConnectionPool() {
        lock.lock();
        try {
            idleConnection = new LinkedList<HbaseConnection>();
            busyConnection = new LinkedList<HbaseConnection>();
            HbaseConnection connection;
            int ret = 0;
            for (int i = 0; i < hbaseClusterConfig.getPoolSize(); ++i) {
                connection = new HbaseConnection();
                ret = connection.initConnection(hbaseClusterConfig);
                if (0 != ret) {
                    logger.warn("init connection failed.");
                }
                idleConnection.add(connection);
                logger.debug("add connection success");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            lock.unlock();
        }

    }

    public HbaseConnection getConnection() {
        lock.lock();
        try {
            while (idleConnection.isEmpty()) {
                notEmpty.await(hbaseClusterConfig.getWaitTimeMillis(), TimeUnit.MILLISECONDS);
            }

            if (idleConnection.isEmpty()) {
                logger.warn("no idle connection");
                return null;
            }

            logger.debug("get connection. before idle pool :" + idleConnection.size());
            HbaseConnection connection;
            connection = idleConnection.get(0);
            idleConnection.remove(connection);
            busyConnection.add(connection);
            logger.debug("get connection. after idle pool :" + idleConnection.size());
            logger.debug("get connection from pool success");
            return connection;
        } catch (InterruptedException e) {
            // do nothing
        } finally {
            lock.unlock();
        }
        return null;
    }

    public synchronized void releaseConnection(HbaseConnection connection) {
        lock.lock();
        try {
            logger.debug("before busy size :" + busyConnection.size() + "idle size :" + idleConnection.size());
            idleConnection.add(connection);
            busyConnection.remove(connection);
            logger.debug("after busy size :" + busyConnection.size() + "idle size :" + idleConnection.size());
            logger.debug("release connection success");
            notEmpty.signal();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

    }


}