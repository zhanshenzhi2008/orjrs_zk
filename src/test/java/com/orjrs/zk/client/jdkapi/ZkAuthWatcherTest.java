package com.orjrs.zk.client.jdkapi;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author orjrs
 * @create 2020-03-01 20:18
 * @since 1.0.0
 */
public class ZkAuthWatcherTest {

    public static final String ORJRS_WATCHER = "/orjrs_watcher";
    public static final String DATA = "我是自定义监控测试节点";
    public static final String UPDATE_DATA = "我是自定义监控测试节点->更新后的数据";

    @Test
    public void test() {
        ZkAuthWatcher zkAuthWatcher = new ZkAuthWatcher();
        zkAuthWatcher.authZk();
    }

}