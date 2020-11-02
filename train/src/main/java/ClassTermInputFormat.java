import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class ClassTermInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext
            taskAttemptContext) throws IOException {

        // 使用了自定义的ClassTermRecordReader来对输入格式进行限定
        ClassTermRecordReader reader = new ClassTermRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
