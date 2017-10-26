package com.bigdata.es.laosiji.estools.esdoc;

import net.sf.json.JSONObject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsDocInit {
    /**
     * json字符串 创建索引数据
     * @param client
     * @param indexname
     * @param type
     * @param json
     * @return
     */
    public static IndexResponse CreateIndexResponse(Client client,String indexname,String type,String json){
        IndexResponse indexResponse = client.prepareIndex(indexname, type)
                .setSource(json, XContentType.JSON)
                .get();
        return indexResponse;
    }

    /**
     * XContentBuilder 创建索引数据
     * @param client
     * @param indexname
     * @param type
     * @param builder
     * @return
     * @throws IOException
     */
    public static IndexResponse CreateIndexResponse(Client client,String indexname,String type,XContentBuilder builder) throws IOException {
       /* XContentBuilder builder1 = jsonBuilder()
                .startObject()
                .field("id", "101")
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();*/
        IndexResponse indexResponse = client.prepareIndex(indexname, type)
                .setSource(builder.string(), XContentType.JSON)
                .get();
        return indexResponse;
    }

    /**
     * javabean 创建索引数据
     * @param client
     * @param indexname
     * @param type
     * @param javaBean
     * @return
     */
    public static IndexResponse CreateIndexResponse(Client client,String indexname,String type,Object javaBean){
        JSONObject jsStr = JSONObject.fromObject(javaBean);
        IndexResponse indexResponse = client.prepareIndex(indexname, type)
                .setSource(jsStr.toString(), XContentType.JSON)
                .get();
        return indexResponse;
    }

    /**
     * 根据ID查询
     * @param client
     * @param indexname
     * @param type
     * @param id
     * @return
     */
    public static GetResponse GetResponseByDocId(Client client,String indexname,String type,String id){
        GetResponse response = client.prepareGet(indexname, type, id).get();
        return response;
    }


    /**
     * 多索引类型查询
     * @param client
     * @return
     */
    public static MultiGetResponse getMultiGetResponse(Client client){

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
               /* .add("twitter", "tweet", "1")
                .add("twitter", "tweet", "2", "3", "4")
                .add("another", "type", "foo")*/
                .add("twitter", "tweet", "1")
                .get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
            }
        }
        return multiGetItemResponses;
    }


    /**
     * 批量添加索引
     * @param client
     * @param index
     * @param type
     * @param obj
     * @return
     */
    public static BulkResponse BulkRequestBuilderCreateIndex(Client client, String index, String type, List<Object> obj){
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(int i=0;i<obj.size();i++){
            //业务对象
            String json = obj.get(i).toString();
            IndexRequestBuilder indexRequest = client.prepareIndex(index, type).setSource(json);
            //添加到builder中
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
        return  bulkResponse;
    }

    public static BulkResponse BulkRequestBuilderDeleteIndex(Client client, String index, String type, List<Object> obj){
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(int i=0;i<obj.size();i++){
            //业务对象
            String json = obj.get(i).toString();
            DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index, type,json);
            //添加到builder中
            bulkRequest.add(deleteRequestBuilder);
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
        return  bulkResponse;
    }

    public static BulkResponse testDemo(Client client) throws IOException {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .endObject()
                )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
        return bulkResponse;
    }


    /**
     * 根据主键删除
     * @param client
     * @param indexname
     * @param type
     * @param id
     * @return
     */
    public static DeleteResponse deleteResponse(Client client,String indexname,String type,String id){
        DeleteResponse response = client.prepareDelete(indexname, type, id).get();
        return  response;
    }

    /**
     * 根据查询结果集删除
     * @param client
     * @param indexname
     * @param keyword
     * @return
     */
    public static Long DeleteByQuery(Client client,String indexname,String keyword){
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery("gender", keyword))
                        .source(indexname)
                        .get();

        long deleted = response.getDeleted();
        return deleted;
    }

    /**
     * 根据查询结果集删除
     * @param client
     * @param indexname
     * @param keyword
     */
    public static void  DeleteByqueryFillter(Client client,String indexname,String keyword){
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("gender", keyword))
                .source(indexname)
                .execute(new ActionListener<BulkByScrollResponse>() {
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                    }
                    public void onFailure(Exception e) {
                        // Handle the exception
                    }
                });
    }

    /**
     * 根据查询结果更新
     * @param client
     * @param index
     * @param type
     * @param id
     * @param builder
     * @return
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static RestStatus updateByRequestByQuery(Client client, String index, String type, String id, XContentBuilder builder) throws IOException, ExecutionException, InterruptedException {
       /*  XContentBuilder builder1 = jsonBuilder()
                .startObject()
                .field("id", "101")
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();*/
        UpdateRequest updateRequest = new UpdateRequest()
                .index(index)
                .type(type)
                .id(id)
                .doc(builder);
        RestStatus restStatus = client.update(updateRequest).get().status();
        return restStatus;
    }

    /**
     *
     * @param client
     * @param index
     * @param type
     * @param id
     * @param builder
     * @return
     */
    public static RestStatus updateByResponse(Client client, String index, String type, String id, XContentBuilder builder){
       UpdateResponse updateResponse = client.prepareUpdate(index, type, id)
                .setDoc(builder)
                .get();
        return updateResponse.status();
    }

    /**
     *
     * @param client
     * @param index
     * @param type
     * @param id
     * @param builder
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static RestStatus updateRequestByIndexRequest(Client client, String index, String type, String id, XContentBuilder builder) throws ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(index, type, id)
                .source(builder);
        UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                .doc(builder)
                .upsert(indexRequest);
        RestStatus restStatus = client.update(updateRequest).get().status();
        return restStatus;
    }
}
