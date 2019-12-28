package com.orjrs.zk.client.jdkapi;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author orjrs
 * @create 2019-12-28 17:45
 * @since 1.0.0
 */
public class ZkSessionTest {

    @Test
    public void createSession() {

        ZkSession zkSession = new ZkSession();
        zkSession.createSession();
    }

    @Test
    public void createSession2() throws Exception {
        ZkSession zkSession = new ZkSession();
        zkSession.createSession2();
    }
}