//package mapreduce.LyricsMerge;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//
//public class LyricsMergeMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
//
//    @Override
//    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
//        context.write(key, value);
//    }
//
//}
