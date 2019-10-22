package com.briup.basic;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.sql.DriverManager;
import java.util.concurrent.CountDownLatch;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/25 10:01
 * 4
 */
public class ZKConnection {

    private static CountDownLatch signal = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        //连接zk
        String hosts = "192.168.25.164:2181";
        int timeout = 1000;
        //水儿子你是我的死儿子
        ZooKeeper zooKeeper = new ZooKeeper(hosts, timeout, event -> {
            System.out.println(event.getType());
            System.out.println(event.getState());
            if (Watcher.Event.KeeperState.SyncConnected.compareTo(event.getState()) == 0) {
                System.out.println("连接成功");
                signal.countDown();
            }

        });
        signal.await();
        System.out.println("hello world");
        zooKeeper.close();
    }
}