import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class PredictTestRecordReader extends RecordReader<Text, Text> {
    FileSplit fileSplit;
    Configuration conf;
    Text key = new Text();
    Text value = new Text();
    Boolean flag = true;

    // 创建了LineRecordReader
    LineRecordReader reader = new LineRecordReader();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
        reader.initialize(inputSplit, taskAttemptContext);
    }

    // 对key和value都做了改变
    // key是测试文件的不带.txt后缀的文件名和类名的组合，并用&分隔二者
    // value是文件中所有的单词
    @Override
    public boolean nextKeyValue() throws IOException {
        if (flag) {
            String result = "";
            while (reader.nextKeyValue()) {
                result += reader.getCurrentValue() + "\n";
            }
            key.set(Util.getFilename(fileSplit.getPath().toString()) +
                    "&" + Util.getTestClassname(fileSplit.getPath().toString()));
            value.set(result);
            flag = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    @Override
    public void close() {

    }
}
