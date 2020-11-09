//package mapreduce.LyricsMerge;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import java.io.IOException;
//
//
//public class LyricsMergeDriver extends Configured implements Tool {
//
//    private final static String MONGO_IP = "10.66.188.17";
//    private final static String MONGO_DB = "semantic";
//    private final static String MONGO_COLLECTION = "";
//
//    public static void main(String[] args) throws Exception {
//        System.exit(ToolRunner.run(new LyricsMergeDriver(), args));
//    }
//
//    @Override
//    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = getConf();
//        conf.set("mongo_ip", MONGO_IP);
//        conf.set("mongo_db", MONGO_DB);
//        conf.set("mongo_collection", MONGO_COLLECTION);
//        conf.set("split", args[2]);
//        conf.set("cursor_limit", String.valueOf(100000));
//
//        Job job = Job.getInstance(conf);
//
//        job.setJarByClass(LyricsMergeDriver.class);
//        job.setMapperClass(LyricsMergeMapper.class);
//        job.setReducerClass(LyricsMergeReducer.class);
//
//        job.setCombinerClass(LyricsMergeReducer.class);
//
//        job.setMapOutputValueClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);
//
//        return job.waitForCompletion(true)?0:1;
//    }
//}