package com.orjrs.zk.client.zkclient;

import jdk.nashorn.internal.runtime.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;

/**
 * ZK Client demo
 *
 * @author orjrs
 * @create 2020-03-21 18:00
 * @since 1.0.0
 */
@Slf4j
public class ZkClientTest {
    /** ZK服务端地址 */
    private static final String SERVER_ADDR = "192.168.135.132";

    /** ZK会话超时时间 */
    private static final int SESSION_TIMEOUT = 10000;

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient(SERVER_ADDR, SESSION_TIMEOUT);
        // 1. 创建临时节点
        zkClient.createEphemeral("/orjrs/temp_test");
        // 2. 创建持久节点
        zkClient.createPersistent("/orjrs/persistent", true);
        List<String> children = zkClient.getChildren("/orjrs");
        for (String child : children) {
            String prefix = "/orjrs/";
            String data = zkClient.readData(prefix + child);
            log.info("========节点{}:{}", prefix + child, data);
        }
    }
}
