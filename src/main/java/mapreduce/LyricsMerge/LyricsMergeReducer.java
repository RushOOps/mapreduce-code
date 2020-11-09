//package mapreduce.LyricsMerge;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//
//public class LyricsMergeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//    @Override
//    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//        long sum = 0;
//        for(LongWritable l : values){
//            sum += l.get();
//        }
//        context.write(key, new LongWritable(sum));
//    }
//
//}
//
//
//
