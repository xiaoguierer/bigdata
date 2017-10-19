package com.cn.laosiji.elk.log4j;


import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;

import java.util.Iterator;
import java.util.Map;

/**
 * 现有索引数据：

 index:school

 type:student

 ---------------------------------------------------

 {"grade":"1", "class":"1", "name":"xiao 1"}

 {"grade":"1", "class":"1", "name":"xiao 2"}

 {"grade":"1", "class":"2", "name":"xiao 3"}

 {"grade":"1", "class":"2", "name":"xiao 4"}

 {"grade":"1", "class":"2", "name":"xiao 5"}



 Java分组统计年级和班级学生个数，如SQL: SELECT grade,class,count(1) FROM student GROUP BY grade,class;
 */
public class TestAggregation {

/*
    @Test

    public void testAggregation()

    {

        SearchRequestBuilder srb = NesUtils.getSearcher("school");

        srb.setTypes("student");

        srb.setSearchType(SearchType.COUNT);



        TermsBuilder gradeTermsBuilder = AggregationBuilders.terms("gradeAgg").field("grade");

        TermsBuilder classTermsBuilder = AggregationBuilders.terms("classAgg").field("class");



        gradeTermsBuilder.subAggregation(classTermsBuilder);



        srb.addAggregation(gradeTermsBuilder);



        SearchResponse sr = srb.execute().actionGet();



        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();



        StringTerms gradeTerms = (StringTerms) aggMap.get("gradeAgg");



        Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();



        while(gradeBucketIt.hasNext())

        {

            StringTerms.Bucket gradeBucket = gradeBucketIt.next();

            System.out.println(gradeBucket.getKey() + "年级有" + gradeBucket.getDocCount() +"个学生。");



            StringTerms classTerms = (StringTerms) gradeBucket.getAggregations().asMap().get("classAgg");

            Iterator<StringTerms.Bucket> classBucketIt = classTerms.getBuckets().iterator();



            while(classBucketIt.hasNext())

            {

                StringTerms.Bucket classBucket = classBucketIt.next();

                System.out.println(gradeBucket.getKey() + "年级" +classBucket.getKey() + "班有" + classBucket.getDocCount() +"个学生。");

            }

            System.out.println();*/
/*
        }



    }*/
}
