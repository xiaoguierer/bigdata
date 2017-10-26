package com.bigdata.es.laosiji.estools.esindex;

import com.bigdata.es.laosiji.estools.esclient.EsInit;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.List;
import java.util.Map;

public class EsIndex {
    static Log log = LogFactory.getLog(EsInit.class);

    /**
     * 判断集群中{index}是否存在   方式一
     * @param index
     * @return 存在（true）、不存在（false）
     */
    public static IndicesExistsResponse indexExists(IndicesAdminClient adminClient, String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = adminClient.exists(request).actionGet();
       return response;
    }


    /**
     * 判断索引是否存在
     * @param client
     * @param index
     * @return
     * 方式二
     */
    public static IndicesExistsResponse isIndexExists(Client client, String index) {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesExistsResponse response = indicesAdminClient.prepareExists(index).get();
        return response;
    }
    /**
     * 判断类型是否存在
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static TypesExistsResponse isTypeExists(Client client, String index, String type) {
        if(!isIndexExists(client, index).isExists()){
            log.info("--------- isTypeExists 索引 [{}] 不存在");
        }
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        TypesExistsResponse response = indicesAdminClient.prepareTypesExists(index).setTypes(type).get();
        return response;
    }




    /**
     * 创建空索引  默认setting 无mapping
     * @param client
     * @param index
     * @return
     */
    public static CreateIndexResponse createSimpleIndex(Client client, String index){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        CreateIndexResponse response = indicesAdminClient.prepareCreate(index).get();
        return response;
    }

    public static CreateIndexResponse createSimpleSetingsIndex(Client client, String index,int n_shards,int n_replicas){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        CreateIndexResponse response = indicesAdminClient.prepareCreate(index)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", n_shards)
                        .put("index.number_of_replicas", n_replicas)
                )
                .get();
        return response;
    }
    /**
     * 创建索引 指定setting
     * @param client
     * @param index
     * @return
     */
    public static CreateIndexResponse createStructureIndex(Client client, String index,int n_shards,int n_replicas,XContentBuilder mappingBuilder){
        // settings
        Settings settings = Settings.builder().put("index.number_of_shards",n_shards).put("index.number_of_replicas", n_replicas).build();
        // mapping
       /* XContentBuilder mappingBuilder;
        try {
            mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                     .startObject("pii-test")
                    .startObject("properties")
                    .startObject("name").field("type", "string").field("store", "yes").endObject()
                    .startObject("sex").field("type", "string").field("store", "yes").endObject()
                    .startObject("college").field("type", "string").field("store", "yes").endObject()
                    .startObject("age").field("type", "integer").field("store", "yes").endObject()
                    .startObject("school").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (Exception e) {
            log.error("--------- createIndex 创建 mapping 失败：",e);
            return false;
        }*/
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        CreateIndexResponse createIndexResponse = indicesAdminClient.prepareCreate(index)
                .setSettings(settings)
                .addMapping(index, mappingBuilder)
                .get();
        return createIndexResponse;
        //return response.isAcknowledged();
    }

    /**
     * 刷新所有索引
     * @param client
     * @return
     */

    public static RefreshResponse refreshIndexs(Client client){
        RefreshResponse refreshResponse = null;
        refreshResponse = client.admin().indices().prepareRefresh().get();
        return refreshResponse;
    }

    /**
     * 刷新特定索引
     * @param client
     * @param index
     * @return
     */
    public static RefreshResponse refreshIndex(Client client,String index){
        RefreshResponse refreshResponse = null;
        refreshResponse = client.admin().indices().prepareRefresh(index).get();
        return refreshResponse;
    }

    /**
     * 刷新多个索引
     * @param client
     * @param index
     * @param index1
     * @return
     */
    public static RefreshResponse refreshIndexs(Client client,String index,String index1){
        RefreshResponse refreshResponse = null;
        refreshResponse = client.admin().indices().prepareRefresh(index,index1).get();
        return refreshResponse;
    }


    /**
     * 更新索引设置
     * @param client
     * @param index
     * @param n_shards
     * @param n_replicas
     * @return
     */
    public  static UpdateSettingsResponse getUpdateSettingsResponse(Client client, String index,int n_shards,int n_replicas){
        UpdateSettingsResponse updateSettingsResponse = null;
        updateSettingsResponse =  client.admin().indices().prepareUpdateSettings(index)
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", n_shards)
                        .put("index.number_of_replicas", n_replicas)
                )
                .get();
        return updateSettingsResponse;
    }
    /**
     * 获取索引设置SETING
     * @param client
     * @param index
     * @return
     */
    public static ImmutableOpenMap getIndexSettings(Client client, String index){
        GetSettingsResponse response = client.admin().indices()
                .prepareGetSettings(index).get();
        ImmutableOpenMap<String,Settings> map = response.getIndexToSettings();
        for (ObjectObjectCursor<String, Settings> cursor :map) {
            String index1 = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
        }
        return map;
    }




    /**
     * 删除索引
     * @param client
     * @param index
     */
    public static DeleteIndexResponse deleteIndex(Client client, String index) {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        DeleteIndexResponse response = indicesAdminClient.prepareDelete(index).execute().actionGet();
        return response;
    }
    /**
     * 关闭索引
     * @param client
     * @param index
     * @return
     */
    public static CloseIndexResponse closeIndex(Client client, String index){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        CloseIndexResponse response = indicesAdminClient.prepareClose(index).get();
        return response;
    }

    /**
     * 打开索引
     * @param client
     * @param index
     * @return
     */
    public static OpenIndexResponse openIndex(Client client, String index){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        OpenIndexResponse response = indicesAdminClient.prepareOpen(index).get();
        return response;
    }

    /**
     * 为索引创建别名
     * @param client
     * @param index
     * @param alias
     * @return
     */
    public static IndicesAliasesResponse addAliasIndex(Client client, String index , String alias){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesAliasesResponse response = indicesAdminClient.prepareAliases().addAlias(index, alias).get();
        return response;
    }

    /**
     * 判断别名是否存在
     * @param client
     * @param aliases
     * @return
     */
    public static AliasesExistResponse isAliasExist(Client client, String... aliases){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        AliasesExistResponse response = indicesAdminClient.prepareAliasesExist(aliases).get();
        return response;
    }

    /**
     * 删除别名
     * @param client
     * @param index
     * @param aliases
     * @return
     */
    public static IndicesAliasesResponse deleteAliasIndex(Client client, String index, String... aliases){
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesAliasesResponse response = indicesAdminClient.prepareAliases().removeAlias(index, aliases).get();
        return response;
    }
    /**
     * 索引统计
     * @param client
     * @param index
     */
    public static void indexStats(Client client, String index) {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesStatsResponse response = indicesAdminClient.prepareStats(index).all().get();
        ShardStats[] shardStatsArray = response.getShards();
        for(ShardStats shardStats : shardStatsArray){
            log.info("shardStats {}"+ shardStats.toString());
        }
        Map<String, IndexStats> indexStatsMap = response.getIndices();
        for(String key : indexStatsMap.keySet()){
            log.info("indexStats {}"+indexStatsMap.get(key));
        }
        CommonStats commonStats = response.getTotal();
        log.info("total commonStats {}"+commonStats.toString());
        commonStats = response.getPrimaries();
        log.info("primaries commonStats {}"+commonStats.toString());
    }
}
