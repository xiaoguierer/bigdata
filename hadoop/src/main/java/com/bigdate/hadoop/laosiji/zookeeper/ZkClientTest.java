package com.bigdate.hadoop.laosiji.zookeeper;

import org.I0Itec.zkclient.*;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * ZkClient
 */
public class ZkClientTest {
    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("192.168.19.130:2181,192.168.19.130:2182,192.168.19.130:2183");
        String node = "/myapp";

        // 订阅监听事件
        childChangesListener(zkClient, node);
        dataChangesListener(zkClient, node);
        stateChangesListener(zkClient);

        if (!zkClient.exists(node)) {
            zkClient.createPersistent(node, "hello zookeeper");
        }
        System.out.println(zkClient.readData(node));

        zkClient.updateDataSerialized(node, new DataUpdater<String>() {

            public String update(String currentData) {
                return currentData + "-123";
            }
        });
        System.out.println(zkClient.readData(node));

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 订阅children变化
     *
     * @param zkClient
     * @param path
     * @author SHANHY
     * @create  2016年3月11日
     */
    public static void childChangesListener(ZkClient zkClient, final String path) {
        zkClient.subscribeChildChanges(path, new IZkChildListener() {

            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("clildren of path " + parentPath + ":" + currentChilds);
            }

        });
    }

    /**
     * 订阅节点数据变化
     *
     * @param zkClient
     * @param path
     * @author SHANHY
     * @create  2016年3月11日
     */
    public static void dataChangesListener(ZkClient zkClient, final String path){
        zkClient.subscribeDataChanges(path, new IZkDataListener(){

            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println("Data of " + dataPath + " has changed.");
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("Data of " + dataPath + " has changed.");
            }

        });
    }

    /**
     * 订阅状态变化
     *
     * @param zkClient
     * @author SHANHY
     * @create  2016年3月11日
     */
    public static void stateChangesListener(ZkClient zkClient){
        zkClient.subscribeStateChanges(new IZkStateListener() {

            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                System.out.println("handleStateChanged");
            }

            public void handleSessionEstablishmentError(Throwable error) throws Exception {
                System.out.println("handleSessionEstablishmentError");
            }

            public void handleNewSession() throws Exception {
                System.out.println("handleNewSession");
            }
        });
    }
}
