package com.bigdata.spark.laosiji.sparktools.sparkstreaming;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 应用场景：我们使用Streaming实时监听指定的Hdfs目录，当该目录有新的文件增加会读取它，
 * 并完成单词计数的操作。
 这里和上一篇的差别就是：上一篇用的是socketTextStream而这里用的是：textFileStream。
 其他没有不同。
 代码展示：
 */
public class SparkStreamDfs {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("NetworkWordCount")
                .set("spark.testing.memory", "2147480000");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        System.out.println(jssc);
        //创建监听文件流
        JavaDStream<String> lines=jssc.textFileStream("hdfs://192.168.61.128:9000/sparkStreaminput001/");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                System.out.println(Arrays.asList(x.split(" ")).get(0));
                return Arrays.asList(x.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        wordCounts.print();
        wordCounts.dstream().saveAsTextFiles("hdfs://192.168.61.128:9000/sparkStream001/wordCount/", "spark");
        jssc.start();
        jssc.awaitTermination();
    }
}
