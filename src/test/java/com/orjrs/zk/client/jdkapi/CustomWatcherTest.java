package com.orjrs.zk.client.jdkapi;

import lombok.extern.log4j.Log4j2;
import org.junit.Assert;
import org.junit.Test;

/**
 * Zk自定义监听器
 *
 * @author orjrs
 * @create 2020-02-23 20:13
 * @since 1.0.0
 */
public class CustomWatcherTest {

    public static final String ORJRS_WATCHER = "/orjrs_watcher";
    public static final String DATA = "我是自定义监控测试节点";
    public static final String UPDATE_DATA = "我是自定义监控测试节点->更新后的数据";

    @Test
    public void test() {
        CustomWatcher customWatcher = new CustomWatcher();
        customWatcher.createConnection();
        String path = customWatcher.createPath(ORJRS_WATCHER, DATA);
        if (ORJRS_WATCHER.equals(path)) {
            String actual = customWatcher.readPath(ORJRS_WATCHER);
            Assert.assertEquals(DATA, actual);
            customWatcher.writePath(ORJRS_WATCHER, UPDATE_DATA);
            String update = customWatcher.readPath(ORJRS_WATCHER);
            Assert.assertEquals(UPDATE_DATA, update);
            customWatcher.deletePath(ORJRS_WATCHER);
            String delete = customWatcher.readPath(ORJRS_WATCHER);
            Assert.assertNull(delete);
        }

    }
}
