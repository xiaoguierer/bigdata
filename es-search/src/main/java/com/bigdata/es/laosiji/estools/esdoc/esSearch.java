package com.bigdata.es.laosiji.estools.esdoc;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class esSearch {
    /**
     *  MultiSearch是ElasticSearch提供的针对多个查询请求进行一次查询的接口，该接口虽然能解决同时执行多个不同的查询，但存在以下问题：
     1. 无法对最终结果进行分页，除非人工分页；
     2. 有可能多个SearchRequest查询出来的结果中，存在重复的结果，但MultiSearch并不负责去重。
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static MultiSearchResponse MultiSearch(Client client,String index,String type){
        SearchRequestBuilder srb1 = client
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);

        SearchRequestBuilder srb2 = client
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);


        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();

        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
        }
        return sr;
    }






    /**
     *
     * @param client
     * @param keywords
     * @return
     */
    public static SearchResponse AggregationSampleSearch(Client client,String index,String type,String keywords){
        SearchResponse sr = client
                .prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms(keywords)
                                .field("field")
                )
                .addAggregation(
                        AggregationBuilders.dateHistogram(keywords)
                                .field("birth")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                )
                .get();

// Get your facet results
        Terms agg1 = sr.getAggregations().get(keywords);
        Histogram agg2 = sr.getAggregations().get(keywords);
        return sr;
    }

    /**
     * 分组查询
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static SearchResponse AggregationGroupBySearch(Client client,String index,String type){
        SearchRequestBuilder sbuilder = client.prepareSearch(index).setTypes(type);
        //    select team, count(*) as player_count from player group by team;
        TermsAggregationBuilder teamAgg= AggregationBuilders.terms("player_count ").field("team");
        sbuilder.addAggregation(teamAgg);
        SearchResponse response = sbuilder.execute().actionGet();
        return response;
    }

    /**
     * 多个分组查询
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static SearchResponse AggregationMuliGroupBySearch(Client client,String index,String type){
        SearchRequestBuilder sbuilder = client.prepareSearch(index).setTypes(type);
        //        select team, position, count(*) as pos_count from player group by team, position;
        TermsAggregationBuilder teamAgg= AggregationBuilders.terms("player_count ").field("team");
        TermsAggregationBuilder posAgg = AggregationBuilders.terms("pos_count").field("position");
        sbuilder.addAggregation(teamAgg.subAggregation(posAgg));
        SearchResponse response = sbuilder.execute().actionGet();
        return response;
    }

    /**
     * 单个求和 最大 最小  平均
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static SearchResponse AggregationOperationBySearch(Client client,String index,String type){
        SearchRequestBuilder sbuilder = client.prepareSearch(index).setTypes(type);
        //            select team, max(age) as max_age from player group by team;
        TermsAggregationBuilder teamAgg= AggregationBuilders.terms("player_count ").field("team");
        MaxAggregationBuilder ageAgg= AggregationBuilders.max("max_age").field("age");
        sbuilder.addAggregation(teamAgg.subAggregation(ageAgg));
        SearchResponse response = sbuilder.execute().actionGet();
        return response;
    }

    /**
     * 多个求求和 最大 最小  平均
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static SearchResponse AggregationOperationMulitBySearch(Client client,String index,String type){
        SearchRequestBuilder sbuilder = client.prepareSearch(index).setTypes(type);
        //     select team, avg(age)as avg_age, sum(salary) as total_salary from player group by team;
        TermsAggregationBuilder teamAgg= AggregationBuilders.terms("player_count ").field("team");
        MaxAggregationBuilder ageAgg= AggregationBuilders.max("max_age").field("age");
        AvgAggregationBuilder salaryAgg= AggregationBuilders.avg("total_salary ").field("salary");
        sbuilder.addAggregation(teamAgg.subAggregation(ageAgg.subAggregation(salaryAgg)));
        SearchResponse response = sbuilder.execute().actionGet();
        return response;
    }

    /**
     * 分组排序
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static SearchResponse AggregationOrderBySearch(Client client,String index,String type){
        SearchRequestBuilder sbuilder = client.prepareSearch(index).setTypes(type);
        //select team, sum(salary) as total_salary from player group by team order by total_salary desc;
        TermsAggregationBuilder teamAgg= AggregationBuilders.terms("team").order((List<Terms.Order>) Histogram.Order.aggregation("total_salary ", false));
        MaxAggregationBuilder ageAgg= AggregationBuilders.max("max_age").field("age");
        AvgAggregationBuilder salaryAgg= AggregationBuilders.avg("total_salary ").field("salary");
        sbuilder.addAggregation(teamAgg.subAggregation(ageAgg.subAggregation(salaryAgg)));
        SearchResponse response = sbuilder.execute().actionGet();
        //
       /* Map<String, Aggregation> aggMap = response.getAggregations().asMap();
        StringTerms gradeTerms = (StringTerms) aggMap.get("keywordAgg");
        Iterator<StringTerms.Bucket> teamBucketIt = gradeTerms .getBuckets().iterator();
        while (teamBucketIt .hasNext()) {
            Terms.Bucket buck = teamBucketIt .next();
            //球队名
            String team = (String) buck.getKey();
            //记录数
            long count = buck.getDocCount();
            //得到所有子聚合
            Map subaggmap = buck.getAggregations().asMap();
            //avg值获取方法
            double avg_age= ((InternalAvg) subaggmap.get("avg_age")).getValue();
            //sum值获取方法
            double total_salary = ((InternalSum) subaggmap.get("total_salary")).getValue();
            //...
            //max/min以此类推
        }*/
        return response;
    }

    /**
     * 综上，聚合操作主要是调用了SearchRequestBuilder的addAggregation方法，通常是传入一个TermsBuilder，子聚合调用TermsBuilder的subAggregation方法，
     * 可以添加的子聚合有TermsBuilder、SumBuilder、AvgBuilder、MaxBuilder、MinBuilder等常见的聚合操作。
     从实现上来讲，SearchRequestBuilder在内部保持了一个私有的 SearchSourceBuilder实例， SearchSourceBuilder内部包含一个List<AbstractAggregationBuilder>，
     每次调用addAggregation时会调用 SearchSourceBuilder实例，添加一个AggregationBuilder。
     同样的，TermsBuilder也在内部保持了一个List<AbstractAggregationBuilder>，调用addAggregation方法（来自父类addAggregation）
     时会添加一个AggregationBuilder。有兴趣的读者也可以阅读源码的实现。
     */









    /**
     * 模板查询
     * @param client
     * @param template_params
     * @return
     */
    public static SearchResponse  TemplateSearch(Client client,Map<String, Object> template_params){
        SearchResponse sr = new SearchTemplateRequestBuilder(client)
                .setScript("template_gender")
                .setScriptType(ScriptType.FILE)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();
        return sr;
    }
}
