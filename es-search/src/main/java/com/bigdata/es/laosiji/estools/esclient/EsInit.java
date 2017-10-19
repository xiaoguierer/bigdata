package com.bigdata.es.laosiji.estools.esclient;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/** * 初始化连接es服务端,这里相当于dao层.
 *
 */
public class EsInit {
    static Log log = LogFactory.getLog(EsInit.class);
    /**
     * 静态,单例...
     */
    private static TransportClient client;
    private static IndicesAdminClient adminClient;

    private static final String CLUSTER_NAME = "pii-es";
    private static final String CLUSTER_NODE_NAME = "pii-es-one";
    private static final String IP = "192.168.0.209";
    private static final int PORT = 9300;  //端口
    public static TransportClient initESClient() {
        try {
            if (client == null) {
                // 配置你的es,现在这里只配置了集群的名,默认是elasticsearch,跟服务器的相同
                Settings settings = Settings.builder()
                        .put("cluster.name", CLUSTER_NAME)
                        .put("discovery.type", "zen")//发现集群方式
                       /* .put("discovery.zen.minimum_master_nodes", 1)//最少有1个master存在
                        .put("discovery.zen.ping_timeout", "200ms")//集群ping时间，太小可能会因为网络通信而导致不能发现集群
                        .put("discovery.initial_state_timeout", "500ms")
                        .put("gateway.type", "local")// (fs, none, local)
                        .put("index.number_of_shards", 1)
                        .put("action.auto_create_index", false)//配置是否自动创建索引
                        .put("cluster.routing.schedule", "50ms")//发现新节点时间
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
                        .put("refresh_interval", "10s")//刷新时间
                        .put("client.transport.sniff", true)*/
                        .build();
                client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("方法AppCommentAction-deleteAppComment,参数信息：commentid");
                log.error("获取客户端对象异常：" + e.getMessage());
            }
        }
        return client;
    }

    public static TransportClient initNodeClient(){
        try{
            if(client == null){
                TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("方法AppCommentAction-deleteAppComment,参数信息：commentid");
                log.error("获取客户端对象异常：" + e.getMessage());
            }
        }
        return client;
    }

    /**
     *
     * @param client
     * @return
     */
    public static AdminClient getAdminClient(Client client){
        return client.admin();
    }
    /**
     * 关闭连接
     */
    public static void closeESClient() {
        if (client != null) {
            client.close();
        }
    }





    /**
     * 设置映射
     * @param client
     * @param index
     * @param type
     * @return
     */

    public static boolean putIndexMapping(Client client, String index, String type){
        // mapping
        XContentBuilder mappingBuilder;
        try {
            mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(type)
                    .startObject("properties")
                    .startObject("name").field("type", "string").field("store", "yes").endObject()
                    .startObject("sex").field("type", "string").field("store", "yes").endObject()
                    .startObject("college").field("type", "string").field("store", "yes").endObject()
                    .startObject("age").field("type", "long").field("store", "yes").endObject()
                    .startObject("school").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (Exception e) {
          //  log.error("--------- createIndex 创建 mapping 失败：", e);
            return false;
        }
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        PutMappingResponse response = indicesAdminClient.preparePutMapping(index).setType(type).setSource(mappingBuilder).get();
        return response.isAcknowledged();
    }
}





