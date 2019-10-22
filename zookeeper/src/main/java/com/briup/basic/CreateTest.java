package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/6/25 11:18
 * 4
 */
public class CreateTest extends ConnectionWatcher {


    public CreateTest(String address) {
        super(address);
    }
    public void create_syn (String path,String data) throws KeeperException, InterruptedException {
        String name=zk.create(path,data.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println("同步创建节点"+name);

    }
    public void create_asyn(String path,String data) throws InterruptedException {
        AsyncCallback.StringCallback sc = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx//从回调对象外部传入的对象
             , String name) {  //创建znode路径，不带序号
                switch (KeeperException.Code.get(rc))
                {
                    case OK:
                        System.out.println("异步创建成功"+name+""+path);
                        System.out.println("传外部传入对象"+ctx);
                        break;

                    case NODEEXISTS:
                        System.out.println("节点存在");
                        break;

                }


            }
        };
        zk.create(path,data.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,sc,"hello");
        Thread.sleep(5000);

    }

    public static void main(String args[]) throws Exception {

        CreateTest  createTest = new CreateTest("192.168.25.164:2181");
        createTest.connect();
        createTest.create_syn("/node_syn","hello");
        createTest.create_asyn("/node_asyn","world");
        createTest.close();

    }
}
