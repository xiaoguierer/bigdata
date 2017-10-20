package com.bigdata.es.laosiji.estools.EsDoc;

import com.bigdata.es.laosiji.estools.esclient.EsInit;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.util.List;

public class EsDocAdd {
    protected static void buildIndex(User user) throws Exception {
        // INDEX_DEMO_01_MAPPING为上个方法中定义的索引,prindextype为类型.jk8231为id,以此可以代替memchche来进行数据的缓存
        IndexResponse response = EsInit.initESClient().prepareIndex()
                .setSource(
                        User.getXContentBuilder(user)
                )
                .setTTL(8000)//这样就等于单独设定了该条记录的失效时间,单位是毫秒,必须在mapping中打开_ttl的设置开关
                .execute()
                .actionGet();
    }

    /**
     * 批量添加记录到索引
     *
     * @param userList 批量添加数据
     * @throws java.io.IOException IOException
     */
    protected static void buildBulkIndex(List<User> userList) throws IOException {
        BulkRequestBuilder bulkRequest = EsInit.initESClient().prepareBulk();
        // either use Es_Setting.client#prepare, or use Requests# to directly build index/delete requests

        for (User user : userList) {
            //通过add批量添加
            bulkRequest.add(EsInit.initESClient().prepareIndex()
                    .setSource(
                            User.getXContentBuilder(user)
                    )
            );
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        //如果失败
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            System.out.println("buildFailureMessage:" + bulkResponse.buildFailureMessage());
        }
    }
}
