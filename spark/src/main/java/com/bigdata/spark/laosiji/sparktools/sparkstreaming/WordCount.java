package com.bigdata.spark.laosiji.sparktools.sparkstreaming;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("E://hello.txt");
        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            public Iterable<String> call(String str) throws Exception {
                return Arrays.asList(str.split(" "));
            }
        });
        JavaPairRDD<String, Integer> word2int = word.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairRDD<String, Integer> res = word2int.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        res.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            private static final long serialVersionUID = 1L;

            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+" : "+t._2);
            }
        });
    }
}
