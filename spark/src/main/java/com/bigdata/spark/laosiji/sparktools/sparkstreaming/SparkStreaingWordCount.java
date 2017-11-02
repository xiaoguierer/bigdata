package com.bigdata.spark.laosiji.sparktools.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 1、开发前准备
 假定您以搭建好了Spark集群。
 2、开发环境采用eclipse maven工程，需要添加Spark Streaming依赖。
 <dependency>
 <groupId>org.apache.spark</groupId>
 <artifactId>spark-streaming_2.10</artifactId>
 <version>1.6.0</version>
 </dependency>
 3、Spark streaming 基于Spark Core进行计算，需要注意事项：
 1.local模式的话,local后必须为大于等于2的数字【即至少2条线程】。因为receiver 占了一个。
 因为,SparkStreaming 在运行的时候,至少需要一条线程不断循环接收数据，而且至少有一条线程处理接收的数据。
 如果只有一条线程的话,接受的数据不能被处理。
 温馨提示：
 对于集群而言，每隔exccutor一般肯定不只一个Thread，那对于处理Spark Streaming应用程序而言，每个executor一般分配多少core比较合适？
 根据我们过去的经验，5个左右的core是最佳的（段子:分配为奇数个core的表现最佳,例如：分配3个、5个、7个core等）
 */
public class SparkStreaingWordCount {
    SparkConf conf = new SparkConf().setMaster("spark：//Master:7077").setAppName("wordCountOnline");
    /**
     * 第二步：创建SparkStreamingContext,这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
     * SparkStreamingContext的构建可以基于SparkConf参数，也可基于持久化的SparkStreamingContext的内容来回复过来
     * （典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
     * 所有需要再Drver重新启动后继续上的状态，此时的状态恢复需要基于曾经的Checkpoint）
     * 2.在一个SparkStreaming应用程序中可以有多个SparkStreamingContext对象，使用下一个SparkStreaming程序之前
     * 需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，我们获得一个启发：
     * SparkStreaming框架也 就是Spark Core上的一个应用程序。
     * 只不过需要Spark工程师写业务逻辑代码。
     * 所以要掌握好Spark，学好SparkStreaming 就行了。
     * java虚拟机在os角度上看就是一个简单的c。c++应用程序。
     * 这里可以用工厂方法创建ssc
     */
    JavaStreamingContext jsc = new  JavaStreamingContext(conf, Durations.seconds(5));
    /**
     * 第三步：创建Spark Streaming 输入数据来源：input Stream
     * 1.数据输入来源可以基于File、HDFS、Flume、Kafka、Socket
     * 2.在这里我们指定数据来源于网络Socket端口,
     * Spark Streaming连接上该端口并在运行的时候一直监听该端口的数据（当然该端口服务首先必须存在，并且在后续会根据业务需要不断的有数据产生）。
     * 有数据和没有数据 在处理逻辑上没有影响（在程序看来都是数据）
     * 3.如果经常在每间隔5秒钟 没有数据的话,不断的启动空的job其实是会造成调度资源的浪费。因为 并没有数据需要发生计算。
     * 所以企业级生产环境的代码在具体提交Job前会判断是否有数据，如果没有的话，就不再提交Job；
     */
    JavaReceiverInputDStream<String> lines = jsc.socketTextStream("Master", 9999);
    /**
     * 第四步：我们就像对RDD编程一样，基于DStream进行编程，原因是DStream是RDD产生的模板，
     * 在Spark Streaming发生计算前，其实质是把每个Batch的DStream的操作翻译成为了RDD操作。
     * DStream对RDD进行了一次抽象。如同DataFrame对RDD进行一次抽象。JavaRDD —-> JavaDStream
     */
    JavaDStream<String> words =  lines.flatMap(new FlatMapFunction<String, String>() {
        public Iterable<String> call(String line) throws Exception {
            // TODO Auto-generated method stub
            return Arrays.asList(line.split(" "));
        }
    });

    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
        private static final long serialVersionUID = 1L;
        public Tuple2<String, Integer> call(String word) throws Exception {
            // TODO Auto-generated method stub
            return new Tuple2<String,Integer>(word,1);
        }
    });

    JavaPairDStream<String, Integer>  wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer v1, Integer v2) throws Exception {
            // TODO Auto-generated method stub
            return v1 + v2;
        }
    });
    /**
     * 此处的print并不会直接触发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的。
     * 具体是否真正触发Job的运行是基于设置的Duration时间间隔的触发的。
     * Spark应用程序要想执行具体的Job对DStream就必须有output Stream操作。
     * ouput Stream 有很多类型的函数触发,类print、savaAsTextFile、saveAsHadoopFiles等，最为重要的是foreachRDD，因为Spark Streamimg处理的结果一般都会
     * 放在Redis、DB、DashBoard等上面，foreachRDD主要
     * 就是用来 完成这些功能的，而且可以随意的自定义具体数据放在哪里。
     */
  /*  wordsCount.print();
    jsc.start();
    jsc.awaitTermination();
    jsc.close();*/
}
