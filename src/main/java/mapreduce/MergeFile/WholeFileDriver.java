package mapreduce.MergeFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class WholeFileDriver extends Configured implements Tool {

    private static final Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {

        BasicConfigurator.configure();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WholeFileDriver.class);
        job.setMapperClass(WholeFileMapper.class);
        job.setReducerClass(WholeFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/Users/crash/Desktop/files"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/crash/Desktop/output"));

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(conf, new WholeFileDriver(), args));
    }
}
