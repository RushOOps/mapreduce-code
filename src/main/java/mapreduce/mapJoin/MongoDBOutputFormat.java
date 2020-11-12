package mapreduce.mapJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class MongoDBOutputFormat extends OutputFormat<NullWritable, Text> {
    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new FileOutputCommitter(null, context);
    }
}
