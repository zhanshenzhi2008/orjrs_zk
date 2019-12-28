package com.orjrs.zk.client.jdkapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 创建zK会话
 *
 * @author orjrs
 * @create 2019-12-22 15:35
 * @since 1.0.0
 */
@Slf4j
public class ZkSession {
    /** ZK服务端地址 */
    private static final String SERVER_ADDR = "192.168.135.132";

    /** ZK会话超时时间 */
    private static final int SESSION_TIMEOUT = 200;

    /** 发令枪 */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 获得session的方式，这种方式可能会在ZooKeeper还没有获得连接的时候就已经对ZK进行访问了
     */
    @Test
    public void createSession() {
        try {

            // 这里watcher不能为空
            // ZooKeeper zooKeeper = new ZooKeeper(SERVER_ADDR, SESSION_TIMEOUT, null);
            ZooKeeper zooKeeper = new ZooKeeper(SERVER_ADDR, SESSION_TIMEOUT, event -> {
                log.info("zookeeper");
                System.out.println(String.format("event! type=%s, stat=%s, path=%s", event.getType(),
                        event.getState(), event.getPath()));
            });
            zooKeeper.create("/orjrs1", "smart".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            String zNode = "/orjrs1";
            log.info("zNode={}", new String(zooKeeper.getData(zNode, false, null)));
        } catch (KeeperException e) {
            log.info("创建ZK session KeeperException： {}", e.getMessage());
        } catch (InterruptedException e) {
            log.info("创建ZK session InterruptedException中断： {}", e.getMessage());
        } catch (IOException e) {
            log.info("创建ZK session IOException： {}", e.getMessage());
        }
    }


    @Test
    public void createSeesion2() throws Exception {
        ZooKeeper zookeeper = new ZooKeeper(SERVER_ADDR, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                // 确认已经连接完毕后再进行操作
                countDownLatch.countDown();
                log.info("zookeeper已经获得了连接");
            }
            log.info("zookeeper");
            System.out.println(String.format("event! type=%s, stat=%s, path=%s", event.getType(),
                    event.getState(), event.getPath()));
        });
        countDownLatch.await();
    }
}
