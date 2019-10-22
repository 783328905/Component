package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Scanner;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/25 20:05
 * 4
 */
public class OptionNode extends ConnectionWatcher {
    public OptionNode(String address) {
        super(address);
    }
    public void delete(String path) throws KeeperException, InterruptedException {

        zk.delete(path,-1);
        System.out.println("删除节点"+ path);



    }
    public void ls(String path) throws KeeperException, InterruptedException {


        List<String> children = zk.getChildren(path, true);
        children.forEach(System.out::println);
    }
    public void set(String path,String data) throws KeeperException, InterruptedException {
        zk.setData(path,data.getBytes(),-1);
        System.out.println(path+"已创建");

    }
    public static void main(String args[]) throws Exception {
        OptionNode node = new OptionNode("192.168.25.164:2181");
        node.connect();;
        node.ls("/");

        //node.delete("/data");
        //node.set("/data","helwewelo");




    }



}
