package com.orjrs.zk.client.jdkapi;

import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.Strings;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义实现Watcher-授权
 *
 * @author orjrs
 * @create 2020-03-01 20:00
 * @since 1.0.0
 */
@Log4j2
public class ZkAuthWatcher implements Watcher {

    /** ZK服务端地址 */
    private static final String SERVER_ADDR = "192.168.135.132";

    /** ZK会话超时时间 */
    private static final int SESSION_TIMEOUT = 10000;

    /** 同步器 */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /** 计时器 */
    AtomicInteger seq = new AtomicInteger();

    /** 标识 */
    private static final String LOG_PREFIX_OF_MAIN = "【ZK_AUTH】";

    /** zk 会话 */
    private ZooKeeper zk = null;

    /** 认证类型 */
    final static String AUTH_TYPE = "digest";

    /** 认证正确密钥 */
    final static String CORRECT_AUTH_KEY = "123456";

    /** 认证错误密钥 */
    final static String BAD_AUTH_KEY = "654321";

    @Override
    public void process(WatchedEvent event) {
        if (event == null) {
            return;
        }
        Event.KeeperState state = event.getState();
        Event.EventType type = event.getType();

        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";
        log.info("{}收到事件通知：{}", logPrefix, state);

        log.info("{}连接状态：{}", logPrefix, state.toString());
        log.info("{}事件类型：{}", logPrefix, type.toString());
        if (Event.KeeperState.SyncConnected == state) {
            log.info("{}成功连接上ZK服务器", logPrefix);
            countDownLatch.countDown();
        } else if (Event.KeeperState.Disconnected == state) {
            log.info("{}与ZK服务器断开连接", logPrefix);
        } else if (Event.KeeperState.AuthFailed == state) {
            log.info("{}权限检查失败", logPrefix);
        } else if (Event.KeeperState.Expired == state) {
            log.info("{}会话失效", logPrefix);
        }
        log.info("--------------------------------------------");
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
            //添加节点授权
            zk.addAuthInfo(AUTH_TYPE, CORRECT_AUTH_KEY.getBytes());
            countDownLatch.await();
            log.info("{}开始连接ZK服务器", LOG_PREFIX_OF_MAIN);
        } catch (InterruptedException e) {
            log.info("{}连接创建失败，发生 InterruptedException={}", LOG_PREFIX_OF_MAIN, e.getMessage());
        } catch (IOException e) {
            log.info("{}连接创建失败，发生 IOException={}", LOG_PREFIX_OF_MAIN, e.getMessage());
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
                log.info("{}关闭ZK连接，发生 InterruptedException={}", LOG_PREFIX_OF_MAIN, e.getMessage());
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
            String result = this.zk.create(path, data.getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            log.info("使用授权key：{},创建节点:{}，初始内容是：{}", CORRECT_AUTH_KEY, path, data);

            return result;
        } catch (KeeperException e) {
            log.info("创建{}节点KeeperException：{}", path, e.getMessage());
        } catch (InterruptedException e) {
            log.info("创建{}节点InterruptedException：{}", path, e.getMessage());
        }
        return null;
    }

    /**
     * 采用正确的密码读取节点信息
     *
     * @param path 路径
     * @param key  密钥
     * @return 节点信息
     */
    public String readPathByAuth(String path, String key) {
        String result = null;
        try {
            if (Strings.isNotBlank(key) && !CORRECT_AUTH_KEY.equals(key)) {
                // 重新创建个连接
                ZooKeeper newZk = null;
                try {
                    newZk = new ZooKeeper(SERVER_ADDR, SESSION_TIMEOUT, this);
                    // 授权
                    newZk.addAuthInfo(AUTH_TYPE, key.getBytes());
                    result = Arrays.toString(newZk.getData(path, false, null));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                // 默认的
                result = new String(this.zk.getData(path, false, null));
                key = CORRECT_AUTH_KEY;
            }
            log.info("使用授权key={}, 读取节点信息成功:{}={}", key, path, result);
            return result;
        } catch (KeeperException e) {
            log.info("key={}, 读取{}节点信息KeeperException：{}", key, path, e.getMessage());
        } catch (InterruptedException e) {
            log.info("key={}, 读取{}节点信息InterruptedException：{}", key, path, e.getMessage());
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

    public void authZk() {
        // 创建连接
        createConnection();

        // 创建节点
        String path = "/orjrs_auth";
        createPath(path, "init content");
        // 创建子节点
        createPath(path + "/deleteNode", "will be deleted");
        // 使用正确的授权方式 默认的
        //readPathByAuth(path, null);
        // 不使用的授权方式
        readPathByAuth(path, "");
        // 使用错误的授权方式
        readPathByAuth(path, BAD_AUTH_KEY);
    }
}
