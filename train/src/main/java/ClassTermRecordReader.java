import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class ClassTermRecordReader extends RecordReader<Text, Text> {
    private Text key = new Text();
    private Text value = new Text();
    FileSplit fileSplit;
    Configuration conf;

    // 定义了一个LineRecordReader
    LineRecordReader reader = new LineRecordReader();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
        reader.initialize(inputSplit, taskAttemptContext);
    }


    // 对key和value都做了改变
    // key是该文档的类名，value是单词
    @Override
    public boolean nextKeyValue() throws IOException {
        if (reader.nextKeyValue()) {
            key.set(Util.getClassname(fileSplit.getPath().toString()));
            value.set(reader.getCurrentValue());
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
    public float getProgress() {
        try {
            return reader.getProgress();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void close() {

    }
}
