package com.bigdata.spark.laosiji.sparktools.sparkrdd;

import com.bigdata.spark.laosiji.sparktools.sparkclient.SparkClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRdd {
    public void rddtest(){
        JavaSparkContext sc = new JavaSparkContext();
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println(distData.collect());
    }

    public static void main(String[] args) throws Exception {
        SparkRdd rdd = new SparkRdd();
        rdd.rddtest();
    }
}
