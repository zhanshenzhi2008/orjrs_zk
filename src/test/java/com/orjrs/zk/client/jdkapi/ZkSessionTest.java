package com.orjrs.zk.client.jdkapi;

import org.junit.Test;

/**
 * @author orjrs
 * @create 2019-12-28 17:45
 * @since 1.0.0
 */
public class ZkSessionTest {

    @Test
    public void createSession1() {

        ZkSession zkSessionDemo = new ZkSession();
        zkSessionDemo.createSession1();
    }

    @Test
    public void createSession() throws Exception {
        ZkSession zkSessionDemo = new ZkSession();
        zkSessionDemo.createSession();
    }
}