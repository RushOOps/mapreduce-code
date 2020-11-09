//package mapreduce.LyricsMerge;
//
//import mapreduce.SemanticCal.MongoDBRecordWriter;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
//
//import java.io.IOException;
//
//public class MongoDBOutputFormat extends OutputFormat<Text, IntWritable> {
//    @Override
//    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext context) {
//        return new MongoDBRecordWriter(context);
//    }
//
//    @Override
//    public void checkOutputSpecs(JobContext jobContext) {
//
//    }
//
//    @Override
//    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
//        // 暂不清楚作用
//        return new FileOutputCommitter(null, context);
//    }
//}
