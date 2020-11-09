package mapreduce.semanticCal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/**
 * 将已经统计过的一个库里的semantic数据按所有字段去重
 */
public class SemanticCalDriver extends Configured implements Tool {

    private static final Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(conf, new SemanticCalDriver(), args));
    }


    @Override
    public int run(String[] args) throws Exception {

        conf.set("mongo_ip", "10.66.188.17");
        conf.set("mongo_db", "semantic");
        conf.set("mongo_collection", "semantic_online_domain_201909~202008");
        conf.set("mongo_collection_to", "semantic_online_domain_201909-202008_v2");
        conf.set("split_num", args[1]);

        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);

        Job job = Job.getInstance(conf);
        job.setJobName(args[0]);

        job.setJarByClass(SemanticCalDriver.class);
        job.setMapperClass(SemanticCalMapper.class);
        job.setReducerClass(SemanticCalReducer.class);
        job.setCombinerClass(SemanticCalReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(MongoDBInputFormat.class);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        return job.waitForCompletion(true)?0:1;
    }
}