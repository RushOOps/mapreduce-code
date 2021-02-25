package mapreduce.TopNPlus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class TopNPlusDriver extends Configured implements Tool {

    private static final Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(conf, new TopNPlusDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        BasicConfigurator.configure();
        conf.set("topN", "2");

        Job job = Job.getInstance(conf);

        job.setJobName("test");
        job.setJarByClass(TopNPlusDriver.class);

        FileInputFormat.addInputPath(job, new Path("/Users/crash/Desktop/topN"));
        job.setMapperClass(TopNPlusMapper.class);
        job.setMapOutputKeyClass(TopNPlusBean.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(TopNPlusPartitioner.class);
        job.setGroupingComparatorClass(TopNPlusComparator.class);
        job.setReducerClass(TopNPlusReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/Users/crash/Desktop/output"));

        return job.waitForCompletion(true)?0:1;
    }
}
