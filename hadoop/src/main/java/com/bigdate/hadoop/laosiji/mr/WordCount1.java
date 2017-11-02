package com.bigdate.hadoop.laosiji.mr;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 *  WordCount是hadoop最经典的一个词频统计方法，它很好的体现了MapReducede分合的思想，在集群中该方法的触发指令为：
 $hadoop jar xxx/xxx/wordcount.jar wordcount  input_path output_path 其中：
 ·      wordcount：为触发的方法名称
 ·      input_path：指定所要统计的数据在hdfs中存放的位置
 ·      output_path：指定对统计后的结果在hdfs中的存放位置
 代码如下：
 */
public class WordCount1 {
    //继承Mapper，对数据进行拆分，其中<Object,Text>为数据的输入类型，<Text,IntWritable>为数据的输出类型
    public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        //map方法的重写，将数据拆分成<word,one>的形式，将数据以<Text,IntWritable>的形式传送到reduce
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString());
            while(it.hasMoreTokens()){
                word.set(it.nextToken());
                context.write(word, one);
            }
        }
    }
    //继承Reducer，通过shuffle阶段获得Map处理后的<word,one>的值，对数据进行汇总合并后以<word,result>的形式输出
    //其中<Text, IntWritable>为输入的格式,<Text,IntWritable>为输出的格式
    public static class IntSumReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        //重写reduce方法，对词频进行统计，其中输入的数据形式为<key,{1，1，1，1}>的形式
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //将相同的key的value值进行相加，得出词频结果
            for(IntWritable val : values){
                sum = sum + val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void wordcountMapReduce(Path input,Path output,Configuration conf) throws IOException, InterruptedException, Exception{
        //建立job任务
        Job job = Job.getInstance(conf,"word count");
        //配置job中的各个类
        job.setJarByClass(WordCount1.class);
        job.setMapperClass(TokenizerMapper.class);
        //combine方法是在reduce之前对map处理结果的一个局部汇总，一般有几个map就会有几个combine
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        //提交任务
        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static void main(String[] arg) throws Exception{
        Configuration conf = new Configuration();
        //从命令行中获取输入输出的路径
        Path input = new Path(arg[1]);
        Path output = new Path(arg[2]);
        //执行mapreduce方法
        wordcountMapReduce(input,output,conf);
    }
    /**
     * 该方法的主要流程为：
     ·      map阶段：将数据进行拆分，拆分成<word,one>的形式
     ·      combine阶段：对每一个map拆分的数据进行一个局部的reduce操作，一般情况下combine函数和reduce是一样的，
              也可以根据自己的需要进行编写
     ·      shuffle阶段：将数据从map拉取到reduce中
     ·      reduce阶段：对所有的输入数据进行合并统计
     MapReduce示例：WordCount（hadoop streaming）
     Hadoop Streaming简单的来说，就是Hadoop提供了Streaming的方法，可以用除java以外的其他语言来编写Mapeduce，也就是说，
     我们可以用除java以外的其他语言编写一个MapReduce方法将其传给Streaming程序，该程序可以创建一个MR任务提交给Hadoop处理。
     用python实现的WordCount程序，在集群中的触发指令为：
     $hadoop jar xxx/xxx/hadoop-streaming.jar -mapper 'python xx/xx/map.py'
     -filexx/xx/map.py                                 #指定map.py执行文件的位置
     -reducer 'python xx/xx/reduce.py'
     -filexx/xx/reduce.py                              #指定reduce.py执行文件的位置
     -input input_path                                   #指定输入文件路径
     -outputoutput_path                              #指定输出文件路径
     -jobconf mapred.reduce.tasks=20        #根据情况来指定reduce的个数
     #注意：在'-mapper '和'-reducer '参数设定中一定要加上python，表示该文件用python编译执行，否则会出现 java.lang.RuntimeException: PipeMapRed.waitOutputThreads()错误。
     */
}
