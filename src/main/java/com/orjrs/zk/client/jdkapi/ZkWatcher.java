package com.orjrs.zk.client.jdkapi;

import javafx.scene.Parent;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Zk监听
 *
 * @author orjrs
 * @create 2020-07-02 20:50
 * @since 1.0.0
 */
@Slf4j
public class ZkWatcher implements Watcher {
    /** ZK服务端地址 */
    private static final String SERVER_ADDR = "192.168.135.132";

    /** ZK会话超时时间 */
    private static final int SESSION_TIMEOUT = 200;

    /** zk变量 */
    private ZooKeeper zk = null;

    /** 信号量 */
    private CountDownLatch cdl = new CountDownLatch(1);

    /** zk父节点 */
    private static final String PARENT_PATH = "/testZkWatch";

    /** zk子节点 */
    private static final String CHILDREN_PATH = "/testZkWatch/children";

    @Override
    public void process(WatchedEvent event) {
        log.info("=====进入监听过程......");
        if (event == null) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(500L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 事件状态
        Event.KeeperState state = event.getState();
        // 事件类型
        Event.EventType type = event.getType();
        if (state == Event.KeeperState.SyncConnected) {
            cdl.countDown();
            log.info("=====成功连接ZK......");
            if (type == Event.EventType.NodeCreated) {
                log.info("=====节点已创建......");
            } else if (type == Event.EventType.NodeDataChanged) {
                log.info("=====节点数据被修改......");
            } else if (type == Event.EventType.NodeDeleted) {
                log.info("=====节点数据被删除......");
            } else if (type == Event.EventType.NodeChildrenChanged) {
                log.info("=====子节点节点数据被修改......");
            }
        } else if (state == Event.KeeperState.ConnectedReadOnly) {
            cdl.countDown();
            log.info("=====成功连接，且只读......");
        } else if (state == Event.KeeperState.AuthFailed) {
            log.info("=====连接授权失败......");
        } else if (state == Event.KeeperState.Disconnected) {
            log.info("=====断开连接......");
        } else if (state == Event.KeeperState.Expired) {
            log.info("=====会话已过期......");
        }
    }


    /**
     * 创建连接
     */
    public void createSession() {
        releaseSession();
        try {
            zk = new ZooKeeper(SERVER_ADDR, SESSION_TIMEOUT, this);
            cdl.await();
        } catch (IOException | InterruptedException e) {
            log.info("=========创建连接失败:{}", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 释放连接
     */
    private void releaseSession() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                log.info("=========关闭连接失败:{}", e.getMessage());
            }
        }
    }

    private void createNode(String path, String data) {
        try {
            zk.create(path, new String(data).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("创建节点{}={}完成", path, data);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getNode(String path) {
        try {
            byte[] data = zk.getData(path, true, null);
            return new String(data);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void deleteNode(String path) {
        try {
            zk.delete(path, -1);
            log.info("删除节点{}完成", path);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void deleteAllNode() {
        try {
            if (zk.exists(CHILDREN_PATH, false) != null) {
                deleteNode(CHILDREN_PATH);
            }
            if (zk.exists(PARENT_PATH, false) != null) {
                deleteNode(PARENT_PATH);
            }
            log.info("删除所有节点完成");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private Stat exists(String path) {
        try {
            Stat stat = zk.exists(path, true);
            log.info("判断节点是否存在{}={}完成", path, stat.toString());
            return stat;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Stat setNode(String path, String data) {
        try {
            Stat stat = zk.setData(path, data.getBytes(), -1);
            log.info("修改节点数据{}={}完成", path, data);
            return stat;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> getChildrenNode(String path) {
        try {
            List<String> children = zk.getChildren(path, true);
            return children;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public static void main(String[] args) throws Exception {
        ZkWatcher zkWatcher = new ZkWatcher();
        zkWatcher.createSession();
        Thread.sleep(1000);
        // 删除所有节点
        zkWatcher.deleteAllNode();

        // 创建节点
        zkWatcher.createNode(PARENT_PATH, "I love my wife");
        zkWatcher.getNode(PARENT_PATH);
        // 如果是临时节点，则不能创建子节点
        zkWatcher.createNode(CHILDREN_PATH, "Baby");
        zkWatcher.getChildrenNode(PARENT_PATH);
    }
}
