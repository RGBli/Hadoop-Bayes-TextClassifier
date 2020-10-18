import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class Prediction {

    private static Hashtable<String, Double> class_prob = new Hashtable();
    private static Hashtable<Map<String, String>, Double> class_term_prob = new Hashtable();
    private static Hashtable<String, Double> class_term_total = new Hashtable();

    //tp,tn,fp,fn,p,r,f1 7个数据结构
    public static Hashtable<String, Integer> TP = new Hashtable();
    public static Hashtable<String, Integer> TN = new Hashtable();
    public static Hashtable<String, Integer> FP = new Hashtable();
    public static Hashtable<String, Integer> FN = new Hashtable();
    public static Hashtable<String, Double> P = new Hashtable();
    public static Hashtable<String, Double> R = new Hashtable();
    public static Hashtable<String, Double> F1 = new Hashtable();


    public Prediction(Configuration conf) throws IOException {
        //计算class_prob
        FileSystem fs1 = FileSystem.get(URI.create(Util.OUTPUT_PATH + "part-r-00000"), conf);
        FSDataInputStream input1;
        LineReader reader1;
        Text line = new Text();

        // 读取先验概率
        input1 = fs1.open(new Path(Util.OUTPUT_PATH + "part-r-00000"));
        reader1 = new LineReader(input1);
        double file_total = 0;
        while (reader1.readLine(line) > 0) {
            String tmp = line.toString();
            String[] args = tmp.split("\t");
            file_total += Double.valueOf(args[1]);
        }


        FileSystem fs2 = FileSystem.get(URI.create(Util.OUTPUT_PATH + "part-r-00000"), conf);
        FSDataInputStream input2;
        LineReader reader2;

        input2 = fs2.open(new Path(Util.OUTPUT_PATH + "part-r-00000"));
        reader2 = new LineReader(input2);
        while (reader2.readLine(line) > 0) {
            String tmp = line.toString();
            String[] args = tmp.split("\t");
            class_prob.put(args[0], Double.valueOf(args[1]) / file_total);
        }


        //计算class-term prob
        FileSystem fs3 = FileSystem.get(URI.create(Util.OUTPUT_PATH1 + "part-r-00000"), conf);
        FSDataInputStream input3;
        LineReader reader3;

        input3 = fs3.open(new Path(Util.OUTPUT_PATH1 + "part-r-00000"));
        reader3 = new LineReader(input3);
        while (reader3.readLine(line) > 0) {
            String tmp = line.toString();
            String[] args = tmp.split("\t");
            double count = Double.valueOf(args[1]);
            String classname = args[0].split("&")[0];
            class_term_total.put(classname, class_term_total.getOrDefault(classname, 0.0) + count);
        }


        FileSystem fs4 = FileSystem.get(URI.create(Util.OUTPUT_PATH1 + "part-r-00000"), conf);
        FSDataInputStream input4;
        LineReader reader4;

        input4 = fs4.open(new Path(Util.OUTPUT_PATH1 + "part-r-00000"));
        reader4 = new LineReader(input4);
        while (reader4.readLine(line) > 0) {
            String tmp = line.toString();
            String[] args = tmp.split("\t");
            double count = Double.valueOf(args[1]);
            String classname = args[0].split("&")[0];
            String term = args[0].split("&")[1];
            Map<String, String> map = new HashMap();
            map.put(classname, term);
            class_term_prob.put(map, count / class_term_total.get(classname));
        }
    }

    public static double conditionalProbabilityForClass(String content, String classname) {
        double result = 0;
        String[] words = content.split("\n");
        for (String word : words) {
            Map<String, String> map = new HashMap();
            map.put(classname, word);
            result += Math.abs(Math.log(class_term_prob.getOrDefault(map, 1.0)));
        }

        result += Math.abs(Math.log(class_prob.get(classname)));

        return result;
    }


    private static class PredictionMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text result = new Text();
            for (String classname : Util.CLASS_NAMES) {
                result.set(classname + "&" + Double.toString(conditionalProbabilityForClass(value.toString(), classname)));
                context.write(key, result);
            }
        }
    }


    private static class PredictionReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String max_classname = "";
            double max_prob = 0;
            for (Text text : values) {
                String[] args = text.toString().split("&");
                if (Double.valueOf(args[1]) > max_prob) {
                    max_prob = Double.valueOf(args[1]);
                    max_classname = args[0];
                }
            }
            context.write(key, new Text(max_classname));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Prediction prediction = new Prediction(conf);
        Job job = Job.getInstance(conf, "prediction");
        job.setJarByClass(Prediction.class);
        job.setMapperClass(PredictionMapper.class);
        job.setReducerClass(PredictionReducer.class);
        job.setInputFormatClass(PredictTestInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (String classname : Util.CLASS_NAMES) {
            FileInputFormat.addInputPath(job, new Path(Util.INPUT_PATH_TEST + classname));
        }
        FileOutputFormat.setOutputPath(job, new Path(Util.OUTPUT_PATH2));
        job.waitForCompletion(false);


        //等job执行完，从outcome2中获取预测结果计算measures
        calculatePrecision(conf);
        for (String classname : Util.CLASS_NAMES) {
            double p = 0, r = 0, f1 = 0, tp = 0, fp = 0, fn = 0;
            tp = TP.getOrDefault(classname, 1);
            fp = FP.getOrDefault(classname, 1);
            fn = FN.getOrDefault(classname, 1);
            System.out.println(tp / fp);
            System.out.println(fp);
            System.out.println(fn);
            p = tp / (tp + fp);
            r = tp / (tp + fn);
            f1 = 2 * p / (p + r);
            P.put(classname, p);
            R.put(classname, r);
            F1.put(classname, f1);
            System.out.println(String.format("%s precision: %f----recall: %f----f1:%f "
                    , classname, p, r, f1));
        }
        printMicroAverage();
        printMacroAverage();
    }


    //根据micro和macro 平均原则计算多个类别的P，R，F1
    private static void printMicroAverage() {
        double sumP = 0, sumR = 0, sumF1 = 0, length = Util.CLASS_NAMES.length;
        for (String classname : Util.CLASS_NAMES) {
            sumP += P.get(classname);
            sumR += R.get(classname);
            sumF1 += F1.get(classname);
        }
        System.out.println(String.format(
                "all classes micro average P: %f", sumP / length));
        System.out.println(String.format(
                "all classes micro average R: %f", sumR / length));
        System.out.println(String.format(
                "all classes micro average F1: %f", sumF1 / length));
    }

    private static void printMacroAverage() {
        double tp = 0, fp = 0, fn = 0;
        double p = 0, r = 0, f1 = 0;
        for (String classname : Util.CLASS_NAMES) {
            tp += TP.get(classname);
            fp += FP.getOrDefault(classname, 1);
            fn += FN.getOrDefault(classname, 1);
        }
        p = tp / (tp + fp);
        r = tp / (tp + fn);
        f1 = 2 * p / (p + r);
        System.out.println(String.format(
                "all classes macro average P: %f", p));
        System.out.println(String.format(
                "all classes macro average R: %f", r));
        System.out.println(String.format(
                "all classes macro average F1: %f", f1));
    }


    //micro vs macro选择micro，取每个类的precision取均值
    //4个记录tp,fp,tn,fn的数据结构，hashtable<class,count>
    //两层循环，第一层每行预测结果，第二层，每个类

    //public static Hashtable<String,Double> PRECISION
    public static void calculatePrecision(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(Util.OUTPUT_PATH2 + "part-r-00000"), conf);
        FSDataInputStream input;
        LineReader reader;
        Text line = new Text();

        // 读取先验概率
        input = fs.open(new Path(Util.OUTPUT_PATH2 + "part-r-00000"));
        reader = new LineReader(input);
        while (reader.readLine(line) > 0) {
            String tmp = line.toString();
            String[] args = tmp.split("\t");
            String[] args1 = args[0].split("&");
            String docid = args1[0];
            String truth = args1[1];
            String predict = args[1];
            for (String classname : Util.CLASS_NAMES) {
                if (truth.equals(classname) && predict.equals(classname)) {
                    TP.put(classname, TP.getOrDefault(classname, 0) + 1);
                } else if (truth.equals(classname) && !predict.equals(classname)) {
                    FN.put(classname, FN.getOrDefault(classname, 0) + 1);
                } else if (!truth.equals(classname) && predict.equals(classname)) {
                    FP.put(classname, FP.getOrDefault(classname, 0) + 1);
                } else if (!truth.equals(classname) && !predict.equals(classname)) {
                    TN.put(classname, TN.getOrDefault(classname, 0) + 1);
                }
            }
        }
    }
}
