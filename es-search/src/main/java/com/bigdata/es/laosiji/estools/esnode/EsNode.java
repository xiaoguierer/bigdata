package com.bigdata.es.laosiji.estools.esnode;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.List;

public class EsNode {
    /**
     * 获取节点信息
     * @return
     */
    public static List<DiscoveryNode> getnodes(TransportClient client){
        List<DiscoveryNode> nodes = client.connectedNodes();
        for (DiscoveryNode node : nodes) {
            System.out.println("节点信息是："+node.getHostAddress());
        }
        return nodes;
    }
}
