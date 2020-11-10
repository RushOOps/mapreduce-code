package mapreduce.groupSort;

import flowBean.IntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class GroupSortDriver extends Configured implements Tool {

    private static final Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        System.exit(ToolRunner.run(conf, new GroupSortDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(conf);

        job.setMapperClass(GroupSortMapper.class);
        job.setReducerClass(GroupSortReducer.class);
        job.setJarByClass(GroupSortDriver.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setPartitionerClass(GroupSortPartitioner.class);
        job.setGroupingComparatorClass(GroupSortComparator.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/crash/Desktop/sort.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/crash/Desktop/output"));

        return job.waitForCompletion(true)?0:1;
    }
}
