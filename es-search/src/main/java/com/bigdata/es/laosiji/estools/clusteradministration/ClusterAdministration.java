package com.bigdata.es.laosiji.estools.clusteradministration;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;

import java.util.Collection;

public class ClusterAdministration {

    /**
     *
     * @param client
     * @return
     */
    public static AdminClient getAdminClient(Client client){
        AdminClient adminClient = client.admin();
        return adminClient;
    }

    /**
     *
     * @param client
     * @return
     */
    public static ClusterAdminClient getClusterAdminClient(Client client){
        ClusterAdminClient clusterAdminClient =ClusterAdministration.getAdminClient(client).cluster();
        return clusterAdminClient;
    }

    /**
     *
     * @param client
     * @return
     */
    public static IndicesAdminClient getIndicesAdminClient(Client client){
        IndicesAdminClient indicesAdminClient = ClusterAdministration.getAdminClient(client).indices();
        return indicesAdminClient;
    }

    /**
     *
     * @param clusterAdminClient
     * @return
     */
    public static ClusterHealthResponse getClusterHealthResponse(ClusterAdminClient clusterAdminClient){
        ClusterHealthResponse healthResponse = clusterAdminClient.prepareHealth().get();
        return  healthResponse;
    }

    /**
     *
     * @param healthResponse
     * @return
     */
    public static String getClusterName(ClusterHealthResponse healthResponse){
        return healthResponse.getClusterName();
    }

    /**
     *
     * @param healthResponse
     * @return
     */
    public static int getNumberOfDataNodes(ClusterHealthResponse healthResponse){
        return healthResponse.getNumberOfDataNodes();
    }

    /**
     *
     * @param healthResponse
     * @return
     */
    public static int getNumberOfNodes(ClusterHealthResponse healthResponse){
        return healthResponse.getNumberOfNodes();
    }

    /**
     *
     * @param healthResponse
     * @return
     */
    public static Collection<ClusterIndexHealth> getClusterIndexHealthList(ClusterHealthResponse healthResponse){
        return (Collection<ClusterIndexHealth>) healthResponse.getIndices().values();
    }

    /**
     *
     * @param clusterIndexHealth
     * @return
     */
    public static String  getIndex(ClusterIndexHealth clusterIndexHealth){
        return clusterIndexHealth.getIndex();
    }

    /**
     *
     * @param clusterIndexHealth
     * @return
     */
    public static int getNumberOfShards(ClusterIndexHealth clusterIndexHealth){
        return clusterIndexHealth.getNumberOfShards();
    }

    /**
     *
     * @param clusterIndexHealth
     * @return
     */
    public static int getNumberOfReplicas(ClusterIndexHealth clusterIndexHealth){
        return clusterIndexHealth.getNumberOfReplicas();
    }

    /**
     *
     * @param clusterIndexHealth
     * @return
     */
    public static ClusterHealthStatus getClusterHealthStatus(ClusterIndexHealth clusterIndexHealth){
        return clusterIndexHealth.getStatus();
    }
}
