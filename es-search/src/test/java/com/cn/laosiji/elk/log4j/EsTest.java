package com.cn.laosiji.elk.log4j;

import com.bigdata.es.laosiji.estools.EsDoc.DataFactory;
import com.bigdata.es.laosiji.estools.clusteradministration.ClusterAdministration;
import com.bigdata.es.laosiji.estools.esclient.EsInit;
import com.bigdata.es.laosiji.estools.esindex.EsIndex;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsTest {

    public static void main(String[] args) throws Exception {
        TransportClient client = EsInit.initESClient();

        System.out.println(client.connectedNodes().size());
       /* XContentBuilder mappingBuilder =  mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("user_name").field("type", "string").field("store", "yes").endObject()
                .startObject("user_sex").field("type", "string").field("store", "yes").endObject()
                .startObject("user_college").field("type", "string").field("store", "yes").endObject()
                .startObject("user_age").field("type", "integer").field("store", "yes").endObject()
                .startObject("user_school").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                .endObject()
                .endObject();
        CreateIndexResponse createIndexResponse = EsIndex.createStructureIndex(client,"pii_test",2,1,mappingBuilder);
        System.out.println(createIndexResponse.isAcknowledged());


        ImmutableOpenMap<String,Settings> map = EsIndex.getIndexSettings(client,"es_piiindex");//es-laosiji  pii_test
        for (ObjectObjectCursor<String, Settings> cursor :map) {
            String index1 = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
        }
        EsIndex.refreshIndexs(client);

        AdminClient adminClient = ClusterAdministration.getAdminClient(client);

        IndicesAdminClient indicesAdminClient = ClusterAdministration.getIndicesAdminClient(client);
        ClusterAdminClient clusterAdminClient = ClusterAdministration.getClusterAdminClient(client);

        ClusterHealthResponse healths = ClusterAdministration.getClusterHealthResponse(clusterAdminClient);

        String clusterName = ClusterAdministration.getClusterName(healths);

        System.out.println("集群名字是： " +  clusterName);
        int numberOfDataNodes = ClusterAdministration.getNumberOfDataNodes(healths);
        System.out.println("集群主节点数：  " +  numberOfDataNodes);
        int numberOfNodes = ClusterAdministration.getNumberOfNodes(healths);
        System.out.println("集群数据节点数： " + numberOfNodes);


        for (ClusterIndexHealth health : ClusterAdministration.getClusterIndexHealthList(healths)) {
            System.out.println("-------------");
            String index = ClusterAdministration.getIndex(health);
            System.out.println("index is " + index);
            int numberOfShards = ClusterAdministration.getNumberOfShards(health);
            System.out.println("nums is " + numberOfShards);
            int numberOfReplicas = ClusterAdministration.getNumberOfReplicas(health);
            System.out.println("numr is " + numberOfReplicas);
            ClusterHealthStatus status = ClusterAdministration.getClusterHealthStatus(health);
            System.out.println("status is " + status);
        }


        ClusterHealthResponse response = clusterAdminClient.prepareHealth()
                .setWaitForYellowStatus()
                .get();
        System.out.println(response.getStatus());
        ClusterHealthResponse response_es_piiindex = clusterAdminClient.prepareHealth("es_piiindex")
                .setWaitForGreenStatus()
                .get();

        ClusterHealthResponse response_es_laosiji = clusterAdminClient.prepareHealth("es-laosiji")
                .setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(2))
                .get();
        ClusterHealthStatus status = response_es_laosiji.getIndices().get("company").getStatus();
        if (!status.equals(ClusterHealthStatus.GREEN)) {
            System.out.println("Index is in " + status + " state");
        }







        String index_name = "es_piiindex";
        IndicesExistsResponse response1 = indicesAdminClient.prepareExists(index_name).get();
        System.out.println(response1.isExists());

        EsIndex.indexStats(client,index_name);

        GetResponse indexresponse = client.prepareGet(index_name, "person", "1")
                .setOperationThreaded(false)
                .get();
        System.out.println(indexresponse.getSourceAsString());

        IndexRequest indexRequest01 = new IndexRequest(index_name, "person", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "张三")
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest(index_name, "person", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("name", "王五")
                        .endObject())
                .upsert(indexRequest01);
        client.update(updateRequest).get();


        UpdateRequest updateRequest1 = new UpdateRequest(index_name, "person", "2")
                .doc(jsonBuilder()
                        .startObject()
                        .field("name", "王五")
                        .field("title", "总监理")
                        .field("desc", "王还降低环境看得见的五")
                        .endObject());
        client.update(updateRequest).get();

        System.out.println(indexresponse.getSourceAsString());*/
    }
    }

