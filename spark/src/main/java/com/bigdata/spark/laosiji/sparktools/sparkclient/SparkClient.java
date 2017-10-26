package com.bigdata.spark.laosiji.sparktools.sparkclient;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.deploy.Client;
import org.apache.spark.deploy.ClientArguments;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.hadoop.conf.Configuration;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkClient {

    public static final String master = "spark://192.168.0.209:7077";
    // ip && mster 做映射
    /**
     * @param appName
     * @return
     */
    public static JavaSparkContext getSparkClient(String appName){
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }


    /**
     * 　第一个参数表示运行方式（local、yarn-client、yarn-standalone等）
     　　 第二个参数表示应用名字
     * @param appName
     * @param runtype
     * @return
     */
    public static JavaSparkContext getSparkClient(String runtype,String appName){
        JavaSparkContext  sc = new JavaSparkContext(runtype,appName);
        return sc;
    }




    /**
     * 单例模式提交
     */
    public static void SubmitSparkStandAlone(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
        String filename = dateFormat.format(new Date());
        String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp =tmp.substring(0, tmp.length()-8);
        String[] arg0=new String[]{
                "--master","spark://node101:7077",
                "--deploy-mode","client",
                "--name","test java submit job to spark",
                "--class","Scala_Test",
                "--executor-memory","1G",
//              "spark_filter.jar",
                tmp+"lib/spark_filter.jar",//
                "hdfs://node101:8020/user/root/log.txt",
                "hdfs://node101:8020/user/root/badLines_spark_"+filename
        };

        SparkSubmit.main(arg0);
    }

    /**
     * spart sbumit on yarn
     */
    public static void SubmitSparkYarnClient(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
        String filename = dateFormat.format(new Date());
        String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();
        tmp =tmp.substring(0, tmp.length()-8);
        String[] arg0=new String[]{
                "--name","test java submit job to yarn",
                "--class","Scala_Test",
                "--executor-memory","1G",
//              "WebRoot/WEB-INF/lib/spark_filter.jar",//
                "--jar",tmp+"lib/spark_filter.jar",//

                "--arg","hdfs://node101:8020/user/root/log.txt",
                "--arg","hdfs://node101:8020/user/root/badLines_yarn_"+filename,
                "--addJars","hdfs://node101:8020/user/root/servlet-api.jar",//
                "--archives","hdfs://node101:8020/user/root/servlet-api.jar"//
        };

//      SparkSubmit.main(arg0);
        Configuration conf = new Configuration();
        String os = System.getProperty("os.name");
        boolean cross_platform =false;
        if(os.contains("Windows")){
            cross_platform = true;
        }
        conf.setBoolean("mapreduce.app-submission.cross-platform", cross_platform);// 配置使用跨平台提交任务
        conf.set("fs.defaultFS", "hdfs://node101:8020");// 指定namenode
        conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
        conf.set("yarn.resourcemanager.address","node101:8032"); // 指定resourcemanager
        conf.set("yarn.resourcemanager.scheduler.address", "node101:8030");// 指定资源分配器
        conf.set("mapreduce.jobhistory.address","node101:10020");

        System.setProperty("SPARK_YARN_MODE", "true");

        SparkConf sparkConf = new SparkConf();
        ClientArguments cArgs = new ClientArguments(arg0);

   //     new Client(cArgs,conf,sparkConf).run();
    }
}
