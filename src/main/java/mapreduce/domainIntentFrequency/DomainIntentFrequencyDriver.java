package mapreduce.domainIntentFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * 管理系统
 * 统计意图领域频次
 * 每日任务
 * 支持增量更新
 */

public class DomainIntentFrequencyDriver {

    private final static String MONGO_IP = "10.66.188.17";
    private final static String MONGO_DB = "semantic";
    private final static String MONGO_DOMAIN_INTENT_TABLE = "semantic_domainIntentFrequency";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        // 设置数据库连接
        conf.set("mongo_ip", MONGO_IP);
        conf.set("mongo_db", MONGO_DB);
        conf.set("mongo_domain_intent_table", MONGO_DOMAIN_INTENT_TABLE);

        // 设置map中间结果压缩
//        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        Job job = Job.getInstance(conf);
        job.setJobName(args[1]);

        job.setJarByClass(DomainIntentFrequencyDriver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(DomainIntentFrequencyMapper.class);
//        job.setCombinerClass(WordFrequencyReducer.class);
        job.setReducerClass(DomainIntentFrequencyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(DomainIntent.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);

    }
}
