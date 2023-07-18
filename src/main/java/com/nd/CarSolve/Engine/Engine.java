package com.nd.CarSolve.Engine;

import javafx.util.Pair;
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
import java.util.*;

import static javafx.collections.FXCollections.sort;


public class Engine {
    public static class EnigneMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(),0, value.getLength(),"UTF-8");

            String Engine = line.split(",",-1)[10];
            if(Engine.length()==0 || Engine.equals("\"Engine")){
                Engine="Other";
            }

            context.write(new Text(Engine),new IntWritable(1));
        }
    }
//    public static class EnigneReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
////        private Map<Text, Integer> r ;
////        @Override
////        protected void setup(Context context) throws IOException, InterruptedException {
////            r = new HashMap<>();
////        }
////        了解了mapreduce的工作原理后，需要新重写函数进行排序操作:对于每个不同的键（key），reduce()方法被调用一次。
//        @Override
//        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            int count=0;
//            for (IntWritable value : values) {
//                count+=value.get();
//            }
//            if(count>=50)
//                context.write(key,new IntWritable(count));
//        }
//    }
    public static class EnigneReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private TreeMap<Integer, List<String>> countMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            countMap = new TreeMap<>(Collections.reverseOrder());
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            if (count >= 50) {
                String engine = key.toString();
                if (countMap.containsKey(count)) {
                    countMap.get(count).add(engine);
                } else {
                    List<String> engines = new ArrayList<>();
                    engines.add(engine);
                    countMap.put(count, engines);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : countMap.entrySet()) {
                int count = entry.getKey();
                List<String> engines = entry.getValue();
                for (String engine : engines) {
                    context.write(new Text(engine), new IntWritable(count));
                }
            }
        }
        //执行顺序：map阶段和示例相同，对每行数据，提取其中的发动机列，得到key值和value=1(用来reduce阶段进行计数)
        //reduce阶段：对于每个不同的键（key），reduce()方法被调用一次；而对于一个相同的键（在map阶段有很多重复的键，其值都是1）
        //Iterable<IntWritable>进行迭代，将数据累加，完成计数功能，同时使用TreeMap，使得计数后大于50的放入TreeMap中，其使用reverseOrder自动完成从大到小的排序
        //cleanup是mapreduce的收尾工作（自动调用），我们将全部reduce后的TreeMap按顺序进行context输出即可。
}
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //args=new String[]{"data/input/cars.csv","data/output/Engine"};
        args=new String[]{"hdfs://hadoop101:8020/car/input/cars.csv","data/output/Engine"};
        //配置对象
        Configuration conf=new Configuration();
        //获取Job对象
        Job job = Job.getInstance(conf);
        //设置jar位置
        job.setJarByClass(Engine.class);
        //关联和Mapper和Reducer
        job.setMapperClass(EnigneMapper.class);
        job.setReducerClass(EnigneReducer.class);
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
