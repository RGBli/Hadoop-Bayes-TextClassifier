import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ClassNameInputFormat extends FileInputFormat<NullWritable, Text> {
    @Override
    public RecordReader<NullWritable, Text> createRecordReader
            (InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {

        // 使用了自定义的ClassNameRecordReader来对输入格式进行限定
        ClassNameRecordReader reader = new ClassNameRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
