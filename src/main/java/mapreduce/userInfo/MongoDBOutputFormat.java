package mapreduce.userInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class MongoDBOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) {
        return new MongoDBRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        // 暂不清楚作用
        return new FileOutputCommitter(null, context);
    }
}
