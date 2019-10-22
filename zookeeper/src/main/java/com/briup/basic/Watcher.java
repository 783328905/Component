package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/26 9:24
 * 4
 */
public class Watcher extends ConnectionWatcher {
    private String path;

    public Watcher(String address) {
        super(address);
    }

    public void setWatcher(String path) throws KeeperException, InterruptedException {
        this.path = path;
        zk.exists(path,this);
        zk.exists(path, true);
    }

    public static void main(String args[]) throws Exception {
        Watcher watcher = new Watcher("192.168.25.164:2181");
        watcher.connect();
        watcher.setWatcher("/node_w");

        Thread.sleep(3000000);
        watcher.close();


    }

    @Override
    public void process(WatchedEvent event) {
        super.process(event);
       try {


           switch (event.getType()) {


               case NodeCreated:
                   System.out.println(event.getPath() + "节点被创建");
                   zk.exists(path, true);
                   break;
               case NodeDeleted:
                   System.out.println(event.getPath() + "节点被删除");
                   zk.exists(path, true);
                   break;
               case NodeDataChanged:
                   System.out.println(event.getPath() + "节点被更改");
                   zk.exists(path, true);
                   break;
               case NodeChildrenChanged:
                   System.out.println(event.getPath() + "子节点被删除");
                   zk.exists(path, true);
                   break;


           }
       }catch (Exception e){
           e.printStackTrace();
       }
    }
}

