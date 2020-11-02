import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ClassNameRecordReader extends RecordReader<NullWritable, Text> {
    private FileSplit fileSplit;
    private Configuration conf;
    private boolean flag = true;
    private Text value = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    // 是这个类中最重要的方法
    // 功能是获取路径中的类名，提取出来给value，并不对key做改变
    @Override
    public boolean nextKeyValue() {
        if (flag) {
            String path = fileSplit.getPath().toString();
            value.set(Util.getClassname(path));
            flag = false;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return null;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return flag ? 1 : 0;
    }

    @Override
    public void close() {

    }
}
