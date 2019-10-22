package com.briup.basic;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/26 14:28
 * 4
 */
public class SimpleClient {
    private static final String host = "192.168.25.164:2181";
    private static final int timeout = 2000;


    ZooKeeper zooKeeper= null;
    @Before
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(host, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType()+"-----"+watchedEvent.getPath());
               /* try {


                     //zooKeeper.getData("/data1",true,null);
                     //zooKeeper.exists("/",true);
                    //zooKeeper.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/


            }
        });
    }

    public static void main(String args[]) throws IOException, KeeperException, InterruptedException {

        //要创建的节点路径



    }
    @Test
    public void create() throws KeeperException, InterruptedException {
        String create = zooKeeper.create("/data1","hello".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        Thread.sleep(Long.MAX_VALUE);

    }
    @Test
    public void getChild() throws KeeperException, InterruptedException, IOException {
        List<String> children = zooKeeper.getChildren("/", true);
        children.forEach(System.out::println);

    }
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/data1",true);
        System.out.println(stat==null?"not exist":"exist");
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getNode() throws KeeperException, InterruptedException {
        System.out.println(new String(zooKeeper.getData("/data1",false,new Stat())));
    }

    @Test
    public void delNode() throws KeeperException, InterruptedException {
        zooKeeper.delete("/data1",-1);
    }
    @Test
    public void setNode() throws KeeperException, InterruptedException {
        zooKeeper.setData("/data1","222".getBytes(),-1);
        getNode();
    }


}
