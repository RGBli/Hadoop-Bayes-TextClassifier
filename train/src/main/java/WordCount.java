import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ClassTermMapper extends
            Mapper<Text, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            word.set(key.toString() + "&" + value.toString());
            context.write(word, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //classname count 作业配置
        Configuration conf = new Configuration();
        Job classCountJob = Job.getInstance(conf, "class count");
        classCountJob.setJarByClass(WordCount.class);
        classCountJob.setMapperClass(TokenizerMapper.class);
        classCountJob.setCombinerClass(IntSumReducer.class);
        classCountJob.setReducerClass(IntSumReducer.class);
        classCountJob.setOutputKeyClass(Text.class);
        classCountJob.setOutputValueClass(IntWritable.class);
        classCountJob.setInputFormatClass(ClassnameInputFormat.class);

        for (String classname : Util.CLASS_NAMES) {
            FileInputFormat.addInputPath(classCountJob, new Path(Util.INPUT_PATH + classname));
        }
        FileOutputFormat.setOutputPath(classCountJob, new Path(Util.OUTPUT_PATH));
        //System.exit(classCountJob.waitForCompletion(true) ? 0 : 1);
        classCountJob.waitForCompletion(true);

        //class-term count 作业配置
        Job classTermCountJob = Job.getInstance(conf, "class-term count");
        classTermCountJob.setJarByClass(WordCount.class);
        classTermCountJob.setMapperClass(ClassTermMapper.class);
        classTermCountJob.setCombinerClass(IntSumReducer.class);
        classTermCountJob.setReducerClass(IntSumReducer.class);
        classTermCountJob.setOutputKeyClass(Text.class);
        classTermCountJob.setOutputValueClass(IntWritable.class);
        classTermCountJob.setInputFormatClass(ClassTermInputFormat.class);

        for (String classname : Util.CLASS_NAMES) {
            FileInputFormat.addInputPath(classTermCountJob, new Path(Util.INPUT_PATH + classname));
        }
        FileOutputFormat.setOutputPath(classTermCountJob, new Path(Util.OUTPUT_PATH1));
        classTermCountJob.waitForCompletion(true);
    }
}