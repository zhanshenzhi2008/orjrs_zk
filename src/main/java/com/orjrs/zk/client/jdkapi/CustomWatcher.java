package com.orjrs.zk.client.jdkapi;

import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 自定义实现Watcher
 *
 * @author orjrs
 * @create 2019-12-28 17:44
 * @since 1.0.0
 */
@Log4j2
public class CustomWatcher implements Watcher {

    /** ZK服务端地址 */
    private static final String SERVER_ADDR = "192.168.135.132";

    /** ZK会话超时时间 */
    private static final int SESSION_TIMEOUT = 10000;

    /** 同步器 */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /** zk 会话 */
    private ZooKeeper zk = null;

    @Override
    public void process(WatchedEvent event) {
        log.info("收到事件通知：{}", event.getState());

        if (Event.KeeperState.SyncConnected == event.getState()) {
            countDownLatch.countDown();
        }
    }

    /**
     * 创建连接
     *
     * @param host    连接地址
     * @param timeOut 超时时间
     */
    public void createConnection(String host, long timeOut) {
        releaseConnection();
        try {
            zk = new ZooKeeper(SERVER_ADDR, SESSION_TIMEOUT, this);
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.info("连接创建失败，发生 InterruptedException={}", e.getMessage());
        } catch (IOException e) {
            log.info("连接创建失败，发生 IOException={}", e.getMessage());
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (null != this.zk) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                // ignore
                log.info("关闭ZK连接，发生 InterruptedException={}", e.getMessage());
            }
        }
    }

    /**
     * 创建连接 -默认
     */
    public void createConnection() {
        createConnection(SERVER_ADDR, SESSION_TIMEOUT);
    }

    /**
     * 创建节点
     *
     * @param path 路径
     * @param data 数据
     */
    public String createPath(String path, String data) {
        try {
            String result = this.zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("读取节点信息成功:{}={}", path, result);
            return result;
        } catch (KeeperException e) {
            log.info("创建{}节点KeeperException：{}", path, e.getMessage());
        } catch (InterruptedException e) {
            log.info("创建{}节点InterruptedException：{}", path, e.getMessage());
        }
        return null;
    }

    /**
     * 读取节点信息
     *
     * @param path 路径
     */
    public String readPath(String path) {
        try {
            String result = new String(this.zk.getData(path, false, null));
            log.info("读取节点信息成功:{}={}", path, result);
            return result;
        } catch (KeeperException e) {
            log.info("读取{}节点信息KeeperException：{}", path, e.getMessage());
        } catch (InterruptedException e) {
            log.info("读取{}节点信息InterruptedException：{}", path, e.getMessage());
        }
        return null;
    }

    /**
     * 删除节点信息
     *
     * @param path 路径
     */
    public void deletePath(String path) {
        try {
            this.zk.delete(path, -1);
            log.info("删除节点信息成功:{}", path);
        } catch (KeeperException e) {
            log.info("删除{}节点信息KeeperException：{}", path, e.getMessage());
        } catch (InterruptedException e) {
            log.info("删除{}节点信息InterruptedException：{}", path, e.getMessage());
        }
    }

    /**
     * 修改节点信息
     *
     * @param path 路径
     * @param data 数据
     */
    public void writePath(String path, String data) {
        try {
            this.zk.setData(path, data.getBytes(), -1);
            log.info("修改节点信息成功:{}={}", path, data);
        } catch (KeeperException e) {
            log.info("修改{}节点信息KeeperException：{}", path, e.getMessage());
        } catch (InterruptedException e) {
            log.info("修改{}节点信息InterruptedException：{}", path, e.getMessage());
        }
    }
}
