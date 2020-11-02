import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    // 计算每个类分别有多少个文档的Mapper
    public static class ClassNameMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text className = new Text();

        // 使用了ClassNameInputFormat类，因此输入的value是ClassNameInputFormat中定义的
        // ClassNameInputFormat中定义了输入的value是类名，输入的key是每个文档
        // 输出的key是类名，value是1
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            className.set(value);
            context.write(value, one);
        }
    }

    // 计算每个类中出现的每个单词的词频
    public static class ClassTermMapper extends
            Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        // 使用了ClassTermInputFormat类，因此输入的key和value是ClassTermInputFormat中定义的
        // ClassTermInputFormat中定义了输入的key是单词，输入的value是类名
        // 输出的key是"类名&单词"的组合，value是1
        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            word.set(key.toString() + "&" + value.toString());
            context.write(word, one);
        }
    }

    // 因为ClassNameMapper和ClassTermMapper两个类的输出类型是一样的
    // 因此共用这一个Reducer
    // 功能是简单加和
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 第一个作业classname count的配置
        Configuration conf = new Configuration();
        Job classCountJob = Job.getInstance(conf, "class count");
        classCountJob.setJarByClass(WordCount.class);
        classCountJob.setMapperClass(ClassNameMapper.class);
        classCountJob.setCombinerClass(IntSumReducer.class);
        classCountJob.setReducerClass(IntSumReducer.class);
        classCountJob.setOutputKeyClass(Text.class);
        classCountJob.setOutputValueClass(IntWritable.class);

        // 设置格式化输入类
        classCountJob.setInputFormatClass(ClassNameInputFormat.class);

        // 设置多个输入路径
        for (String classname : Util.CLASS_NAMES) {
            FileInputFormat.addInputPath(classCountJob, new Path(Util.INPUT_PATH + classname));
        }

        // 设置输出路径
        FileOutputFormat.setOutputPath(classCountJob, new Path(Util.OUTPUT_PATH1));
        // System.exit(classCountJob.waitForCompletion(true) ? 0 : 1);
        classCountJob.waitForCompletion(true);


        // 第二个作业class-term count的配置
        Job classTermCountJob = Job.getInstance(conf, "class-term count");
        classTermCountJob.setJarByClass(WordCount.class);
        classTermCountJob.setMapperClass(ClassTermMapper.class);
        classTermCountJob.setCombinerClass(IntSumReducer.class);
        classTermCountJob.setReducerClass(IntSumReducer.class);
        classTermCountJob.setOutputKeyClass(Text.class);
        classTermCountJob.setOutputValueClass(IntWritable.class);

        // 设置格式化输入类
        classTermCountJob.setInputFormatClass(ClassTermInputFormat.class);

        // 设置多个输入路径
        for (String classname : Util.CLASS_NAMES) {
            FileInputFormat.addInputPath(classTermCountJob, new Path(Util.INPUT_PATH + classname));
        }

        // 设置输出路径
        FileOutputFormat.setOutputPath(classTermCountJob, new Path(Util.OUTPUT_PATH2));
        classTermCountJob.waitForCompletion(true);
    }
}