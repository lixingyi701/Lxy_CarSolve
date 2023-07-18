package com.nd.CarSolve.mpg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class mpg {
    //在mr中需要对mpg进行的数据清洗：
    //1.去除全部为字符串类型的参数（如 Blue Accents")和空值
    //2.去除所有存在0的和超过100的（两个数-->一个数，一个数直接舍去）
    //3.如果有两个数，那两个数取平均数计算；如果有一个数，那直接取该数字
    public static class mpgMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        //每行是一个LongWritable变量,输入键类型，很少被调用
        //Text--输入值类型，主要对其处理
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable,Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //重写 Mapper函数的原始签名，context是上下文对象，用于将输出键值对写入到输出上下文中（应该是reducer中需要被处理的）
            String line=new String(value.getBytes(),0, value.getLength(),"utf-8");
            String mpg=line.split(",",-1)[7];
            //String.split()中的limit参数表示拆分次数，-1表示尽可能多的拆分（有多少","拆分多少）
            if(mpg.length()==5){
                context.write(new Text(mpg),new IntWritable(1));
            }
        }
    }
    public static class mpgReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count=0;
            for (IntWritable value : values) {
                count+=value.get();
            }
            if(count>=50)
                context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args=new String[]{"hdfs://hadoop101:8020/car/input/cars.csv","data/output/mpg"};
        //配置对象
        Configuration conf=new Configuration();
        //获取Job对象
        Job job = Job.getInstance(conf);
        //设置jar位置
        job.setJarByClass(mpg.class);
        //关联和Mapper和Reducer
        job.setMapperClass(mpgMapper.class);
        job.setReducerClass(mpgReducer.class);
        //设置Mapper输出的键值对
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reducer最终输出的键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }



}
